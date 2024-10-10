from markdownify import markdownify as md

def html_to_markdown(html_content):
    markdown_content = md(html_content)
    return markdown_content

# Example usage
html_file = r'old_files/instructions.html'

with open(html_file, 'r') as file:
    html_content = file.read()

markdown_content = html_to_markdown(html_content)

markdown_file = r'instructions.md'
with open(markdown_file, 'w') as file:
    file.write(markdown_content)