def extract_txt_blocks(file_path: str, block_size: int = 5000):
    blocks = []
    current = []
    current_len = 0
    block_index = 1

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            current.append(line)
            current_len += len(line)

            if current_len >= block_size:
                text = "".join(current).strip()
                if text:
                    blocks.append(
                        {
                            "block_number": block_index,
                            "text": text,
                        }
                    )
                    block_index += 1
                current = []
                current_len = 0

    if current:
        text = "".join(current).strip()
        if text:
            blocks.append(
                {
                    "block_number": block_index,
                    "text": text,
                }
            )

    return blocks