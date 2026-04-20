from markitdown import MarkItDown

md = MarkItDown()
result = md.convert("/Users/vincent/Downloads/案件防控培训.pptx")
print(result.text_content)
