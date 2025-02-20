import logging
import os
import shutil
from pathlib import Path

from utils.static_utils import generate_index_html, get_versioned_static_file

logger = logging.getLogger(__name__)


def build_static_files():
    """Build and version static files"""
    static_dir = Path(os.path.join(os.path.dirname(__file__), "web", "static"))
    dist_dir = static_dir / "dist"

    logger.info(f"Building static files in {static_dir}")

    # Create dist directory if it doesn't exist
    dist_dir.mkdir(exist_ok=True)

    logger.info(f"Clearing old files in {dist_dir}")

    # Clear old files
    for file in dist_dir.glob("*"):
        file.unlink()

    logger.info(f"Copying CSS files to {dist_dir}")

    # Copy and version CSS files
    for css_file in (static_dir / "css").glob("*.css"):
        versioned_name = get_versioned_static_file(f"css/{css_file.name}")
        shutil.copy2(css_file, dist_dir / Path(versioned_name).name)

    logger.info(f"Copying JS files to {dist_dir}")

    # Copy and version JS files
    for js_file in (static_dir / "js").glob("*.js"):
        versioned_name = get_versioned_static_file(f"js/{js_file.name}")
        shutil.copy2(js_file, dist_dir / Path(versioned_name).name)

    logger.info("Generating index.html with versioned file references")

    # Generate index.html with versioned file references
    generate_index_html()


if __name__ == "__main__":
    build_static_files()
