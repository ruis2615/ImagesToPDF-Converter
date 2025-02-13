import os
from PIL import Image
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
import glob
import uuid
import tempfile
import re
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from dotenv import load_dotenv

# 環境変数の読み込み
load_dotenv()

def natural_sort_key(s):
    return [int(text) if text.isdigit() else text.lower()
            for text in re.split('([0-9]+)', os.path.basename(s))]

def process_single_image(args):
    try:
        index, image_path = args
        
        # A4サイズの設定（mmをポイントに変換）
        page_width = 210 * mm
        page_height = 297 * mm
        
        # 拡張子の確認
        ext = os.path.splitext(image_path)[1].lower()
        temp_webp = None
        
        if ext not in ['.webp']:
            # WebPに変換
            img = Image.open(image_path)
            temp_webp = os.path.join(tempfile.gettempdir(), f"{uuid.uuid4()}.webp")
            img.save(temp_webp, 'WEBP')
            image_path = temp_webp
        
        # 画像を開く
        img = Image.open(image_path)
        
        # 画像をフィットさせる
        img_width, img_height = img.size
        width_ratio = page_width / img_width
        height_ratio = page_height / img_height
        
        if width_ratio < height_ratio:
            new_width = int(page_width)
            new_height = int(img_height * width_ratio)
        else:
            new_height = int(page_height)
            new_width = int(img_width * height_ratio)
        
        img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
        
        # 一時ファイルとして保存
        output_path = os.path.join(tempfile.gettempdir(), f"{uuid.uuid4()}.webp")
        img.save(output_path, 'WEBP')
        
        # 一時ファイルを削除
        if temp_webp and os.path.exists(temp_webp):
            os.remove(temp_webp)
        
        return {
            'success': True,
            'path': output_path,
            'width': new_width,
            'height': new_height,
            'original_path': image_path,
            'index': index
        }
        
    except Exception as e:
        print(f"Error processing {image_path}: {e}")
        return {
            'success': False,
            'original_path': image_path,
            'error': str(e),
            'index': index
        }

def ensure_directory_exists(directory):
    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
            print(f"Created directory: {directory}")
        except Exception as e:
            print(f"Error creating directory {directory}: {e}")
            raise

def convert_images_to_pdf():
    try:
        # 環境変数から設定を読み込む
        input_dir = os.getenv('INPUT_DIRECTORY', 'input_images')
        output_dir = os.getenv('OUTPUT_DIRECTORY', 'output')
        output_pdf_name = f"{os.getenv('OUTPUT_PDF', 'output')}.pdf"
        max_workers = int(os.getenv('MAX_WORKERS', '10'))
        
        # 出力ディレクトリの確認と作成
        ensure_directory_exists(output_dir)
        
        # 出力PDFのフルパスを作成
        output_pdf = os.path.join(output_dir, output_pdf_name)
        
        # 対応する画像形式
        supported_formats = ['.png', '.jpg', '.jpeg', '.webp']
        
        # 画像ファイルを取得して自然な順序でソート
        image_files = []
        for ext in supported_formats:
            image_files.extend(glob.glob(os.path.join(input_dir, f'*{ext}')))
        
        # 自然な順序でソート
        image_files.sort(key=natural_sort_key)
        
        if not image_files:
            print("No image files found!")
            return

        # インデックス付きのタプルリストを作成
        indexed_files = list(enumerate(image_files))
        
        # 実際のワーカー数を決定（CPU数と最大ワーカー数の小さい方）
        n_workers = min(max_workers, cpu_count())
        print(f"Using {n_workers} workers for parallel processing")
        
        # PDFキャンバスの作成
        page_width = 210 * mm
        page_height = 297 * mm
        c = canvas.Canvas(output_pdf, pagesize=(page_width, page_height))
        
        # 並列処理用のプール作成
        processed_files = []
        with Pool(processes=n_workers) as pool:
            # tqdmで進捗バーを表示しながら並列処理を実行
            for result in tqdm(pool.imap(process_single_image, indexed_files), 
                             total=len(indexed_files), 
                             desc="Processing images"):
                if result['success']:
                    processed_files.append(result)
        
        # インデックスでソート
        processed_files.sort(key=lambda x: x['index'])
        
        # PDFを生成
        print("Generating PDF...")
        for result in processed_files:
            try:
                # PDFページの中央に配置
                x = int((page_width - result['width']) / 2)
                y = int((page_height - result['height']) / 2)
                c.drawImage(result['path'], x, y, result['width'], result['height'])
                c.showPage()
            except Exception as e:
                print(f"Error adding image to PDF: {e}")
            finally:
                # 一時ファイルを削除
                if os.path.exists(result['path']):
                    os.remove(result['path'])
        
        # PDFを保存
        c.save()
        print(f"PDF generation completed! Saved to: {output_pdf}")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    convert_images_to_pdf()