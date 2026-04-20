from markitdown import MarkItDown

md = MarkItDown()
result = md.convert("/Users/vincent/Downloads/The-Complete-Guide-to-Building"
                    "-Skill-for-Claude.pdf")
print(result.text_content)
