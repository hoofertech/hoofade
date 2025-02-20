import hashlib
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


def get_file_hash(filepath: str) -> str:
    """Generate a hash of the file contents"""
    with open(filepath, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()[:8]


def get_versioned_static_file(filename: str) -> str:
    """Get the versioned filename for a static file"""
    static_dir = Path(os.path.join(os.path.dirname(__file__), "..", "web", "static"))
    filepath = static_dir / filename

    if not filepath.exists():
        return filename

    file_hash = get_file_hash(str(filepath))
    basename = os.path.basename(filepath)
    name, ext = os.path.splitext(basename)
    return f"{name}.{file_hash}{ext}"


def generate_index_html():
    """Generate index.html with versioned static files"""
    web_dir = Path(os.path.join(os.path.dirname(__file__), "..", "web"))
    template_path = web_dir / "templates" / "index.html"
    output_path = web_dir / "static" / "index.html"

    # Get versioned filenames
    css_filename = get_versioned_static_file("css/styles.css")
    js_filename = get_versioned_static_file("js/app.js")

    logger.info(f"CSS filename: {css_filename}")
    logger.info(f"JS filename: {js_filename}")

    # Read template
    with open(template_path, "r") as f:
        template = f.read()

    # Replace placeholders with versioned filenames
    html = template.replace(
        'href="/static/css/styles.css"', f'href="/static/dist/{css_filename}"'
    ).replace('src="/static/js/app.js"', f'src="/static/dist/{js_filename}"')

    # Write output
    with open(output_path, "w") as f:
        f.write(html)
