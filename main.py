import os
from PIL import Image
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
from reportlab.lib.pagesizes import A4
import glob
import uuid
import tempfile
import re
from multiprocessing import Pool, cpu_count, Manager
from concurrent.futures import ThreadPoolExecutor
from reportlab.lib.utils import ImageReader
import io
from tqdm import tqdm
from dotenv import load_dotenv
import logging
import time

# 環境変数の読み込み
load_dotenv()

# 画質設定
WEBP_QUALITY = 80  # WebP変換時の品質（0-100）
PDF_DPI = 300      # PDF出力時のDPI
IMAGE_DPI = 300    # 画像処理時のDPI
CHUNK_SIZE = 5     # 並列処理時のチャンクサイズ

# ロギング設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def natural_sort_key(s):
    """自然な並び順のためのキー関数"""
    return [int(text) if text.isdigit() else text.lower()
            for text in re.split('([0-9]+)', os.path.basename(s))]

def optimize_image(img):
    """画像の最適化処理"""
    if img.mode in ['RGBA', 'LA']:
        background = Image.new('RGB', img.size, 'WHITE')
        if img.mode == 'RGBA':
            background.paste(img, mask=img.split()[3])
        else:
            background.paste(img, mask=img.split()[1])
        return background
    elif img.mode != 'RGB':
        return img.convert('RGB')
    return img

def needs_resize(img_size, target_size, dpi):
    """リサイズが必要かどうかを判定"""
    img_width, img_height = img_size
    target_width = int(target_size[0] * dpi / 72)
    target_height = int(target_size[1] * dpi / 72)
    
    # サイズ差が1%以内なら、リサイズ不要と判断
    width_diff = abs(img_width - target_width) / target_width
    height_diff = abs(img_height - target_height) / target_height
    
    return width_diff > 0.01 or height_diff > 0.01

def process_image_chunk(chunk_data):
    """チャンク単位で画像を処理"""
    chunk_results = []
    for index, image_path in chunk_data:
        try:
            # 拡張子を確認
            is_webp = image_path.lower().endswith('.webp')
            
            # 画像を開く
            with Image.open(image_path) as img:
                # DPI情報の取得
                dpi = img.info.get('dpi', (IMAGE_DPI, IMAGE_DPI))
                if isinstance(dpi, tuple):
                    dpi = dpi[0]

                # サイズ計算
                width_ratio = A4[0] / img.width
                height_ratio = A4[1] / img.height

                if width_ratio < height_ratio:
                    new_width = int(A4[0] * dpi / 72)
                    new_height = int(img.height * width_ratio * dpi / 72)
                else:
                    new_height = int(A4[1] * dpi / 72)
                    new_width = int(img.width * height_ratio * dpi / 72)

                # リサイズが必要か確認
                if needs_resize(img.size, (new_width, new_height), dpi):
                    img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                    needs_conversion = True
                else:
                    needs_conversion = not is_webp

                # WebPでない場合または変換が必要な場合のみ最適化と変換を実行
                if needs_conversion:
                    img = optimize_image(img)
                    img_buffer = io.BytesIO()
                    img.save(img_buffer, format='WEBP', quality=WEBP_QUALITY, method=6)
                else:
                    # WebPの場合は直接バッファにコピー
                    with open(image_path, 'rb') as f:
                        img_buffer = io.BytesIO(f.read())

                img_buffer.seek(0)

                chunk_results.append({
                    'success': True,
                    'buffer': img_buffer,
                    'width': new_width * 72 / dpi,
                    'height': new_height * 72 / dpi,
                    'index': index,
                    'original_format': 'WEBP' if is_webp else img.format,
                    'converted': needs_conversion
                })

        except Exception as e:
            logging.error(f"Error processing {image_path}: {e}")
            chunk_results.append({
                'success': False,
                'error': str(e),
                'index': index
            })

    return chunk_results

def create_pdf_from_buffers(processed_results, output_pdf):
    """メモリ上の画像バッファからPDFを生成"""
    c = canvas.Canvas(output_pdf, pagesize=A4)
    c.setPageCompression(0)

    # インデックスでソート
    processed_results.sort(key=lambda x: x['index'])

    for result in processed_results:
        if result['success']:
            try:
                # PDFページの中央に配置
                x = (A4[0] - result['width']) / 2
                y = (A4[1] - result['height']) / 2

                # BytesIOからImageReaderオブジェクトを作成
                img_reader = ImageReader(Image.open(result['buffer']))
                
                # 画像を配置
                c.drawImage(
                    img_reader,
                    x, y,
                    result['width'],
                    result['height'],
                    preserveAspectRatio=True
                )
                c.showPage()

            except Exception as e:
                logging.error(f"Error adding image to PDF: {e}")

            finally:
                # バッファをクローズ
                result['buffer'].close()

    c.save()


def convert_images_to_pdf():
    """画像ファイルをPDFに変換する（最適化版）"""
    start_time = time.time()
    
    try:
        # 環境変数から設定を読み込む
        input_dir = os.getenv('INPUT_DIRECTORY', 'input_images')
        output_dir = os.getenv('OUTPUT_DIRECTORY', 'output')
        output_pdf_name = f"{os.getenv('OUTPUT_PDF', 'output')}.pdf"
        max_workers = int(os.getenv('MAX_WORKERS', '10'))

        # 出力ディレクトリの作成
        os.makedirs(output_dir, exist_ok=True)
        output_pdf = os.path.join(output_dir, output_pdf_name)

        # 画像ファイルの収集
        supported_formats = ['.png', '.jpg', '.jpeg', '.webp']
        image_files = []
        for ext in supported_formats:
            image_files.extend(glob.glob(os.path.join(input_dir, f'*{ext}')))
        
        
        # 自然な順序でソート
        image_files.sort(key=natural_sort_key)
        

        # 自然な順序でソート
        image_files.sort(key=natural_sort_key)
        
        if not image_files:
            logging.warning("No image files found!")
            return

        # 自然な順序でソート
        image_files.sort(key=natural_sort_key)
        indexed_files = list(enumerate(image_files))

        # チャンクに分割
        chunks = [indexed_files[i:i + CHUNK_SIZE] 
                 for i in range(0, len(indexed_files), CHUNK_SIZE)]

        # ワーカー数の決定
        n_workers = min(max_workers, cpu_count(), len(chunks))
        logging.info(f"Processing with {n_workers} workers")

        # 統計情報の初期化
        converted_count = 0
        skipped_count = 0

        # 並列処理
        all_results = []
        with Pool(processes=n_workers) as pool:
            with tqdm(total=len(image_files), desc="Processing images") as pbar:
                for chunk_results in pool.imap_unordered(process_image_chunk, chunks):
                    for result in chunk_results:
                        if result['success']:
                            if result.get('converted', True):
                                converted_count += 1
                            else:
                                skipped_count += 1
                    all_results.extend(chunk_results)
                    pbar.update(len(chunk_results))

        # 成功した処理のみを抽出
        successful_results = [r for r in all_results if r['success']]

        # PDF生成
        logging.info("Generating PDF...")
        create_pdf_from_buffers(successful_results, output_pdf)

        end_time = time.time()
        processing_time = end_time - start_time
        
        # 処理結果の表示
        logging.info(f"Processing completed in {processing_time:.2f} seconds")
        logging.info(f"Total images processed: {len(image_files)}")
        logging.info(f"Images converted: {converted_count}")
        logging.info(f"Conversions skipped: {skipped_count}")
        logging.info(f"PDF saved to: {output_pdf}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    convert_images_to_pdf()