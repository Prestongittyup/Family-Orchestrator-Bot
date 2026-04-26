import os
import argparse
import zipfile

ALLOWED_EXTENSIONS = {
    ".js", ".jsx", ".tsx", ".py", ".java", ".cs", ".cpp", ".c", ".h",
    ".php", ".dart", ".css", ".lua", ".rs", ".pl", ".sql", ".bash", ".sh",
    ".json", ".xml", ".yaml", ".yml", ".ini", ".config", ".utf8",
    ".docx", ".doc", ".docm", ".pdf", ".txt", ".rtf", ".md", ".html", ".htm",
    ".pptx", ".ppsm", ".ppt",
    ".xlsx", ".xls", ".xlsm", ".csv", ".tsv",
    ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff",
    ".log",
    ".loop", ".fluid"
}


def is_allowed(ext):
    return ext in ALLOWED_EXTENSIONS


def collect_allowed_files(root_dir):
    matches = []

    for root, _, files in os.walk(root_dir):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, root_dir)

            _, ext = os.path.splitext(file.lower())

            if is_allowed(ext):
                matches.append((full_path, rel_path))

    return matches


def create_zip(matches, output_zip):
    with zipfile.ZipFile(output_zip, "w", zipfile.ZIP_DEFLATED) as z:
        for full_path, rel_path in matches:
            try:
                z.write(full_path, rel_path)
            except Exception as e:
                print(f"Skipping: {full_path} -> {e}")


def main():
    parser = argparse.ArgumentParser(description="Zip ONLY Copilot-allowed files")
    parser.add_argument("directory", help="Directory to scan")
    parser.add_argument("--output", default="copilot_upload.zip", help="Output zip file")

    args = parser.parse_args()

    matches = collect_allowed_files(args.directory)

    if not matches:
        print("No allowed files found.")
        return

    print(f"\nFound {len(matches)} allowed files.\n")

    # Summary
    summary = {}
    for full_path, _ in matches:
        _, ext = os.path.splitext(full_path.lower())
        summary[ext] = summary.get(ext, 0) + 1

    for ext, count in sorted(summary.items(), key=lambda x: x[1], reverse=True):
        print(f"{ext:15} {count}")

    print(f"\nCreating zip: {args.output}")
    create_zip(matches, args.output)

    print("Done.")


if __name__ == "__main__":
    main()