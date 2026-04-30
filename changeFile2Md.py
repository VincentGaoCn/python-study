from markitdown import MarkItDown
from fastapi import FastAPI, UploadFile, File
import tempfile
import os

app = FastAPI()

@app.post("/convert")
async def convert_file(file: UploadFile = File(...)):
    """
    接收上传的文件并转换为 Markdown 格式
    """
    # 创建临时文件保存上传的内容
    with tempfile.NamedTemporaryFile(delete=False, suffix=f".{file.filename.split('.')[-1] if '.' in file.filename else ''}") as tmp_file:
        # 读取上传文件内容并写入临时文件
        content = await file.read()
        tmp_file.write(content)
        tmp_file_path = tmp_file.name
    
    try:
        # 使用 MarkItDown 转换文件
        md = MarkItDown()
        result = md.convert(tmp_file_path)
        
        return {
            "filename": file.filename,
            "content_type": file.content_type,
            "markdown_content": result.text_content
        }
    except Exception as e:
        return {
            "error": str(e),
            "filename": file.filename
        }
    finally:
        # 清理临时文件
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)

@app.get("/")
def root():
    return {"message": "文件转换服务已启动，请使用 POST /convert 接口上传文件进行转换"}


