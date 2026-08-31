import difflib
import re
import unicodedata
from dataclasses import dataclass, replace
from typing import Any

_TOKEN_RE = re.compile(r"\w+|[^\w\s]", re.UNICODE)
_LIST_RE = re.compile(
    r"^\s*(?:[•*–—-]|\d+[.)]|[A-Za-zА-Яа-яЁё][.)])\s+\S"
)
_NUMBERED_HEADING_RE = re.compile(r"^\d+(?:\.\d+)*\.?\s+\S")
_SENTENCE_END_RE = re.compile(r"[.!?…;:]$")
_STRUCTURAL_PREFIX_RE = re.compile(
    r"^\s*(?:"
    r"#{1,6}\s+|"
    r"[•▪◦‣*–—-]\s+|"
    r"\d+(?:\.\d+)*[.)]\s+|"
    r"[A-Za-zА-Яа-яЁё][.)]\s+"
    r")"
)
_DASH_TRANSLATION = str.maketrans(
    {
        "‐": "-",
        "‑": "-",
        "‒": "-",
        "–": "-",
        "—": "-",
        "−": "-",
    }
)


def normalize_technical(text: str) -> str:
    """Normalize layout artifacts only; meaningful characters stay untouched."""
    text = unicodedata.normalize("NFC", text or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"(?<=\w)\u00ad[ \t]*\n[ \t]*(?=\w)", "", text)
    text = text.replace("\u00ad", "")
    # A hyphen immediately at a visual line boundary is an OCR/layout artifact.
    text = re.sub(r"(?<=\w)-[ \t]*\n[ \t]*(?=\w)", "", text)
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.split("\n")]
    text = "\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def collapse_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", normalize_technical(text)).strip()


def normalize_for_alignment(text: str) -> str:
    """Build a content key without OCR/Word structural decoration."""
    normalized = normalize_technical(text).translate(_DASH_TRANSLATION)
    cleaned_lines: list[str] = []
    for line in normalized.splitlines():
        line = line.strip()
        if not line:
            continue
        line = _STRUCTURAL_PREFIX_RE.sub("", line)
        line = re.sub(r"^\s*(?:\*\*|__|\*)+", "", line)
        line = re.sub(r"(?:\*\*|__|\*)+\s*$", "", line)
        line = line.replace("**", "").replace("__", "")
        line = _STRUCTURAL_PREFIX_RE.sub("", line)
        cleaned_lines.append(line.strip())
    return re.sub(r"\s+", " ", " ".join(cleaned_lines)).strip()


@dataclass(frozen=True)
class SourcePage:
    """One persisted page/chunk with extraction provenance."""

    page_number: int
    text: str
    filename: str = ""
    source_content_type: str = "text"
    was_ocr: bool = False


@dataclass(frozen=True)
class TextBlock:
    index: int
    kind: str
    text: str
    page_start: int = 1
    page_end: int = 1
    block_index: int = 0
    page_block_count: int = 1
    filename: str = ""
    source_content_type: str = "text"
    was_ocr: bool = False
    previous_text: str = ""
    next_text: str = ""

    @property
    def key(self) -> str:
        return normalize_for_alignment(self.text)

    @property
    def literal_key(self) -> str:
        return collapse_whitespace(self.text)


@dataclass(frozen=True)
class BlockAlignment:
    op: str
    file1: TextBlock | None
    file2: TextBlock | None


@dataclass(frozen=True)
class TextToken:
    text: str
    start: int
    end: int


@dataclass(frozen=True)
class DiffCandidate:
    line_number: int | None
    file1_line: str | None
    file2_line: str | None
    evidence1: str
    evidence2: str
    change_tags: tuple[str, ...]
    candidate_id: str = ""
    alignment_id: int = 0
    changed1: str = ""
    changed2: str = ""
    change_position: str = "middle"
    file1_page: int | None = None
    file2_page: int | None = None
    file1_block: int | None = None
    file2_block: int | None = None
    file1_page_block_count: int | None = None
    file2_page_block_count: int | None = None
    file1_block_kind: str | None = None
    file2_block_kind: str | None = None
    file1_source_type: str | None = None
    file2_source_type: str | None = None
    file1_was_ocr: bool = False
    file2_was_ocr: bool = False
    previous1: str = ""
    previous2: str = ""
    next1: str = ""
    next2: str = ""

    def result_dict(
        self,
        *,
        category: str | None = None,
        technical_type: str | None = None,
        reason: str | None = None,
        confidence: float | None = None,
        protection_tags: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "candidate_id": self.candidate_id,
            "line_number": self.line_number,
            "file1_line": self.file1_line,
            "file2_line": self.file2_line,
            "file1_span": None,
            "file2_span": None,
            "category": category,
            "technical_type": technical_type,
            "reason": reason,
            "confidence": confidence,
            "protection_tags": list(protection_tags),
            "file1_page": self.file1_page,
            "file2_page": self.file2_page,
            "file1_block": self.file1_block,
            "file2_block": self.file2_block,
            "file1_source_type": self.file1_source_type,
            "file2_source_type": self.file2_source_type,
        }
        return result

    def classifier_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "location": {
                "file1": {
                    "page": self.file1_page,
                    "block": self.file1_block,
                    "page_block_count": self.file1_page_block_count,
                    "kind": self.file1_block_kind,
                    "source_type": self.file1_source_type,
                    "was_ocr": self.file1_was_ocr,
                },
                "file2": {
                    "page": self.file2_page,
                    "block": self.file2_block,
                    "page_block_count": self.file2_page_block_count,
                    "kind": self.file2_block_kind,
                    "source_type": self.file2_source_type,
                    "was_ocr": self.file2_was_ocr,
                },
            },
            "previous": {
                "file1": self.previous1 or None,
                "file2": self.previous2 or None,
            },
            "paragraph": {
                "file1": self.evidence1 or None,
                "file2": self.evidence2 or None,
            },
            "next": {
                "file1": self.next1 or None,
                "file2": self.next2 or None,
            },
            "changed": {
                "file1": (
                    self.changed1
                    if len(self.changed1) <= 300
                    else self.file1_line
                ) or None,
                "file2": (
                    self.changed2
                    if len(self.changed2) <= 300
                    else self.file2_line
                ) or None,
                "position": self.change_position,
            },
            "change_tags": list(self.change_tags),
        }


class HierarchicalDiffEngine:
    """Structural block alignment followed by token-level literal diff."""

    def compare(
        self,
        file1_text: str,
        file2_text: str,
        *,
        candidate_prefix: str = "candidate",
    ) -> list[DiffCandidate]:
        return self.compare_pages(
            [SourcePage(page_number=1, text=file1_text)],
            [SourcePage(page_number=1, text=file2_text)],
            candidate_prefix=candidate_prefix,
        )

    def compare_pages(
        self,
        file1_pages: list[SourcePage],
        file2_pages: list[SourcePage],
        *,
        candidate_prefix: str = "candidate",
    ) -> list[DiffCandidate]:
        if self._document_literal_key(file1_pages) == self._document_literal_key(
            file2_pages
        ):
            return []

        blocks1 = self._segment_pages(file1_pages)
        blocks2 = self._segment_pages(file2_pages)
        alignments = self._align_blocks(blocks1, blocks2)

        candidates: list[DiffCandidate] = []
        for alignment_id, alignment in enumerate(alignments, start=1):
            if alignment.op == "equal":
                continue
            if alignment.file1 is None:
                assert alignment.file2 is not None
                alignment_candidates = [
                    self._whole_block_candidate(
                        None,
                        alignment.file2,
                        "insert",
                    )
                ]
            elif alignment.file2 is None:
                alignment_candidates = [
                    self._whole_block_candidate(
                        alignment.file1,
                        None,
                        "delete",
                    )
                ]
            else:
                alignment_candidates = self._diff_block_pair(
                    alignment.file1,
                    alignment.file2,
                )

            for candidate in alignment_candidates:
                ordinal = len(candidates) + 1
                candidates.append(
                    replace(
                        candidate,
                        candidate_id=f"{candidate_prefix}:c{ordinal:05d}",
                        alignment_id=alignment_id,
                    )
                )

        return candidates

    @staticmethod
    def _document_literal_key(pages: list[SourcePage]) -> str:
        return collapse_whitespace("\n\n".join(page.text for page in pages))

    @staticmethod
    def to_fragment(candidates: list[DiffCandidate]) -> dict[str, Any]:
        return {
            "identical": not candidates,
            "differences": [candidate.result_dict() for candidate in candidates],
        }

    @staticmethod
    def batch_for_classification(
        candidates: list[DiffCandidate],
        *,
        max_chars: int = 10000,
        max_candidates: int = 6,
    ) -> list[list[DiffCandidate]]:
        batches: list[list[DiffCandidate]] = []
        current: list[DiffCandidate] = []
        current_chars = 0

        for candidate in candidates:
            size = (
                len(candidate.evidence1)
                + len(candidate.evidence2)
                + len(candidate.previous1)
                + len(candidate.previous2)
                + len(candidate.next1)
                + len(candidate.next2)
                + 300
            )
            if current and (
                len(current) >= max_candidates
                or current_chars + size > max_chars
            ):
                batches.append(current)
                current = []
                current_chars = 0
            current.append(candidate)
            current_chars += size

        if current:
            batches.append(current)
        return batches

    batch_for_verification = batch_for_classification

    def _segment_pages(self, pages: list[SourcePage]) -> list[TextBlock]:
        blocks: list[TextBlock] = []
        for page in pages:
            blocks.extend(self._segment_page(page, start_index=len(blocks)))

        enriched: list[TextBlock] = []
        for index, block in enumerate(blocks):
            enriched.append(
                replace(
                    block,
                    index=index,
                    previous_text=blocks[index - 1].text if index > 0 else "",
                    next_text=(
                        blocks[index + 1].text
                        if index + 1 < len(blocks)
                        else ""
                    ),
                )
            )
        return enriched

    def _segment(self, text: str) -> list[TextBlock]:
        """Compatibility helper for unit-level callers."""
        return self._segment_pages([SourcePage(page_number=1, text=text)])

    def _segment_page(
        self,
        page: SourcePage,
        *,
        start_index: int,
    ) -> list[TextBlock]:
        text = normalize_technical(page.text)
        if not text:
            return []

        raw_blocks: list[tuple[str, str]] = []
        paragraph_lines: list[str] = []

        def flush_paragraph() -> None:
            if not paragraph_lines:
                return
            paragraph = " ".join(paragraph_lines).strip()
            paragraph_lines.clear()
            if not paragraph:
                return
            for piece in self._split_long_paragraph(paragraph):
                raw_blocks.append(("paragraph", piece))

        for line in text.split("\n"):
            line = line.strip()
            if not line:
                flush_paragraph()
                continue

            kind = self._special_block_kind(line)
            if kind is not None:
                flush_paragraph()
                raw_blocks.append((kind, line))
                continue

            paragraph_lines.append(line)
            if _SENTENCE_END_RE.search(line) or len(" ".join(paragraph_lines)) >= 4000:
                flush_paragraph()

        flush_paragraph()
        page_block_count = len(raw_blocks)
        return [
            TextBlock(
                index=start_index + block_index,
                kind=kind,
                text=block_text,
                page_start=page.page_number,
                page_end=page.page_number,
                block_index=block_index + 1,
                page_block_count=page_block_count,
                filename=page.filename,
                source_content_type=page.source_content_type,
                was_ocr=page.was_ocr,
            )
            for block_index, (kind, block_text) in enumerate(raw_blocks)
        ]

    @staticmethod
    def _special_block_kind(line: str) -> str | None:
        if line.count("|") >= 1:
            return "table_row"
        if _LIST_RE.match(line):
            return "list_item"

        letters = [char for char in line if char.isalpha()]
        uppercase_ratio = (
            sum(char.isupper() for char in letters) / len(letters)
            if letters
            else 0.0
        )
        if len(line) <= 120 and (
            _NUMBERED_HEADING_RE.match(line)
            or (uppercase_ratio >= 0.7 and not _SENTENCE_END_RE.search(line))
        ):
            return "heading"
        return None

    @staticmethod
    def _split_long_paragraph(text: str, max_chars: int = 4000) -> list[str]:
        if len(text) <= max_chars:
            return [text]

        sentences = re.split(r"(?<=[.!?…;:])\s+", text)
        pieces: list[str] = []
        current = ""
        for sentence in sentences:
            if len(sentence) > max_chars:
                if current:
                    pieces.append(current)
                    current = ""
                for start in range(0, len(sentence), max_chars):
                    pieces.append(sentence[start : start + max_chars])
                continue
            proposed = f"{current} {sentence}".strip()
            if current and len(proposed) > max_chars:
                pieces.append(current)
                current = sentence
            else:
                current = proposed
        if current:
            pieces.append(current)
        return pieces

    def _align_blocks(
        self,
        blocks1: list[TextBlock],
        blocks2: list[TextBlock],
    ) -> list[BlockAlignment]:
        matcher = difflib.SequenceMatcher(
            a=[block.key for block in blocks1],
            b=[block.key for block in blocks2],
            autojunk=False,
        )
        alignments: list[BlockAlignment] = []
        for op, i1, i2, j1, j2 in matcher.get_opcodes():
            left = blocks1[i1:i2]
            right = blocks2[j1:j2]
            if op == "equal":
                alignments.extend(
                    BlockAlignment(
                        (
                            "equal"
                            if block1.literal_key == block2.literal_key
                            else "replace"
                        ),
                        block1,
                        block2,
                    )
                    for block1, block2 in zip(left, right)
                )
            elif op == "delete":
                alignments.extend(
                    BlockAlignment("delete", block, None) for block in left
                )
            elif op == "insert":
                alignments.extend(
                    BlockAlignment("insert", None, block) for block in right
                )
            else:
                alignments.extend(self._align_changed_range(left, right))
        return alignments

    def _align_changed_range(
        self,
        left: list[TextBlock],
        right: list[TextBlock],
    ) -> list[BlockAlignment]:
        """Ordered alignment supporting one-to-many paragraph regrouping."""
        rows = len(left) + 1
        cols = len(right) + 1
        gap_cost = 0.70
        scores = [[float("inf")] * cols for _ in range(rows)]
        previous: list[
            list[tuple[int, int, str, int, int] | None]
        ] = [
            [None] * cols for _ in range(rows)
        ]
        scores[0][0] = 0.0

        for i in range(rows):
            for j in range(cols):
                score = scores[i][j]
                if score == float("inf"):
                    continue

                if i < len(left):
                    candidate = score + gap_cost
                    if candidate < scores[i + 1][j]:
                        scores[i + 1][j] = candidate
                        previous[i + 1][j] = (i, j, "delete", 1, 0)

                if j < len(right):
                    candidate = score + gap_cost
                    if candidate < scores[i][j + 1]:
                        scores[i][j + 1] = candidate
                        previous[i][j + 1] = (i, j, "insert", 0, 1)

                for left_size in range(1, min(3, len(left) - i) + 1):
                    for right_size in range(1, min(3, len(right) - j) + 1):
                        left_group = self._combine_blocks(
                            left[i : i + left_size]
                        )
                        right_group = self._combine_blocks(
                            right[j : j + right_size]
                        )
                        ratio = difflib.SequenceMatcher(
                            a=left_group.key,
                            b=right_group.key,
                            autojunk=False,
                        ).ratio()
                        kind_penalty = (
                            0.06
                            if left_size == right_size == 1
                            and left_group.kind != right_group.kind
                            else 0.0
                        )
                        grouping_penalty = 0.03 * (
                            left_size + right_size - 2
                        )
                        substitution_cost = min(
                            1.35,
                            1.0 - ratio + kind_penalty + grouping_penalty,
                        )
                        candidate = score + substitution_cost
                        next_i = i + left_size
                        next_j = j + right_size
                        if candidate <= scores[next_i][next_j]:
                            scores[next_i][next_j] = candidate
                            previous[next_i][next_j] = (
                                i,
                                j,
                                "replace",
                                left_size,
                                right_size,
                            )

        result: list[BlockAlignment] = []
        i, j = len(left), len(right)
        while i > 0 or j > 0:
            step = previous[i][j]
            if step is None:
                # Defensive fallback; normally only possible at (0, 0).
                if i > 0:
                    result.append(BlockAlignment("delete", left[i - 1], None))
                    i -= 1
                elif j > 0:
                    result.append(BlockAlignment("insert", None, right[j - 1]))
                    j -= 1
                continue

            prev_i, prev_j, op, left_size, right_size = step
            if op == "replace":
                block1 = self._combine_blocks(
                    left[prev_i : prev_i + left_size]
                )
                block2 = self._combine_blocks(
                    right[prev_j : prev_j + right_size]
                )
                result.append(
                    BlockAlignment(
                        (
                            "equal"
                            if block1.literal_key == block2.literal_key
                            else "replace"
                        ),
                        block1,
                        block2,
                    )
                )
            elif op == "delete":
                result.append(BlockAlignment("delete", left[prev_i], None))
            else:
                result.append(BlockAlignment("insert", None, right[prev_j]))
            i, j = prev_i, prev_j

        result.reverse()
        return result

    @staticmethod
    def _combine_blocks(blocks: list[TextBlock]) -> TextBlock:
        if not blocks:
            raise ValueError("Cannot combine an empty block range")
        if len(blocks) == 1:
            return blocks[0]

        first = blocks[0]
        last = blocks[-1]
        source_types = {block.source_content_type for block in blocks}
        kinds = {block.kind for block in blocks}
        return TextBlock(
            index=first.index,
            kind=first.kind if len(kinds) == 1 else "group",
            text=" ".join(block.text for block in blocks),
            page_start=first.page_start,
            page_end=last.page_end,
            block_index=first.block_index,
            page_block_count=last.page_block_count,
            filename=first.filename,
            source_content_type=(
                first.source_content_type
                if len(source_types) == 1
                else "mixed"
            ),
            was_ocr=any(block.was_ocr for block in blocks),
            previous_text=first.previous_text,
            next_text=last.next_text,
        )

    def _diff_block_pair(
        self,
        block1: TextBlock,
        block2: TextBlock,
    ) -> list[DiffCandidate]:
        tokens1 = self._tokens(block1.text)
        tokens2 = self._tokens(block2.text)
        matcher = difflib.SequenceMatcher(
            a=[token.text for token in tokens1],
            b=[token.text for token in tokens2],
            autojunk=False,
        )
        candidates: list[DiffCandidate] = []

        for op, i1, i2, j1, j2 in matcher.get_opcodes():
            if op == "equal":
                continue

            # Separate independent one-to-one replacements so several typos in
            # one paragraph are not collapsed into one vague result.
            if op == "replace" and (i2 - i1) == (j2 - j1):
                for offset in range(i2 - i1):
                    candidates.append(
                        self._token_range_candidate(
                            block1,
                            block2,
                            tokens1,
                            tokens2,
                            i1 + offset,
                            i1 + offset + 1,
                            j1 + offset,
                            j1 + offset + 1,
                        )
                    )
                continue

            candidates.append(
                self._token_range_candidate(
                    block1,
                    block2,
                    tokens1,
                    tokens2,
                    i1,
                    i2,
                    j1,
                    j2,
                )
            )

        if not candidates and block1.key != block2.key:
            # Safety net for an unexpected tokenizer edge case.
            candidates.append(self._whole_block_candidate(block1, block2, "replace"))
        return candidates

    def _token_range_candidate(
        self,
        block1: TextBlock,
        block2: TextBlock,
        tokens1: list[TextToken],
        tokens2: list[TextToken],
        i1: int,
        i2: int,
        j1: int,
        j2: int,
    ) -> DiffCandidate:
        changed1 = self._changed_text(block1.text, tokens1, i1, i2)
        changed2 = self._changed_text(block2.text, tokens2, j1, j2)
        if changed1 and changed2:
            result1 = self._clip(changed1, 200)
            result2 = self._clip(changed2, 200)
        else:
            # For an insertion/deletion, show both surrounding phrases so the
            # absent token has an unambiguous location.
            result1 = self._context(
                block1.text, tokens1, i1, i2, radius=5, max_chars=200
            )
            result2 = self._context(
                block2.text, tokens2, j1, j2, radius=5, max_chars=200
            )
        position = self._change_position(
            len(tokens1),
            len(tokens2),
            i1,
            i2,
            j1,
            j2,
        )
        return DiffCandidate(
            # Structural block numbers are not physical source line numbers.
            line_number=None,
            file1_line=result1,
            file2_line=result2,
            evidence1=self._context(
                block1.text,
                tokens1,
                i1,
                i2,
                radius=max(15, len(tokens1)),
                max_chars=4000,
            ),
            evidence2=self._context(
                block2.text,
                tokens2,
                j1,
                j2,
                radius=max(15, len(tokens2)),
                max_chars=4000,
            ),
            change_tags=self._change_tags(changed1, changed2),
            changed1=changed1,
            changed2=changed2,
            change_position=position,
            file1_page=block1.page_start,
            file2_page=block2.page_start,
            file1_block=block1.block_index,
            file2_block=block2.block_index,
            file1_page_block_count=block1.page_block_count,
            file2_page_block_count=block2.page_block_count,
            file1_block_kind=block1.kind,
            file2_block_kind=block2.kind,
            file1_source_type=block1.source_content_type,
            file2_source_type=block2.source_content_type,
            file1_was_ocr=block1.was_ocr,
            file2_was_ocr=block2.was_ocr,
            previous1=self._clip(block1.previous_text, 600),
            previous2=self._clip(block2.previous_text, 600),
            next1=self._clip(block1.next_text, 600),
            next2=self._clip(block2.next_text, 600),
        )

    def _whole_block_candidate(
        self,
        block1: TextBlock | None,
        block2: TextBlock | None,
        op: str,
    ) -> DiffCandidate:
        text1 = "" if block1 is None else block1.text
        text2 = "" if block2 is None else block2.text
        return DiffCandidate(
            line_number=None,
            file1_line=self._clip(text1, 200) if text1 else None,
            file2_line=self._clip(text2, 200) if text2 else None,
            evidence1=self._clip(text1, 4000),
            evidence2=self._clip(text2, 4000),
            change_tags=tuple(
                sorted({"block", op, *self._change_tags(text1, text2)})
            ),
            changed1=text1,
            changed2=text2,
            change_position="whole_block",
            file1_page=None if block1 is None else block1.page_start,
            file2_page=None if block2 is None else block2.page_start,
            file1_block=None if block1 is None else block1.block_index,
            file2_block=None if block2 is None else block2.block_index,
            file1_page_block_count=(
                None if block1 is None else block1.page_block_count
            ),
            file2_page_block_count=(
                None if block2 is None else block2.page_block_count
            ),
            file1_block_kind=None if block1 is None else block1.kind,
            file2_block_kind=None if block2 is None else block2.kind,
            file1_source_type=(
                None if block1 is None else block1.source_content_type
            ),
            file2_source_type=(
                None if block2 is None else block2.source_content_type
            ),
            file1_was_ocr=False if block1 is None else block1.was_ocr,
            file2_was_ocr=False if block2 is None else block2.was_ocr,
            previous1=(
                "" if block1 is None else self._clip(block1.previous_text, 600)
            ),
            previous2=(
                "" if block2 is None else self._clip(block2.previous_text, 600)
            ),
            next1="" if block1 is None else self._clip(block1.next_text, 600),
            next2="" if block2 is None else self._clip(block2.next_text, 600),
        )

    @staticmethod
    def _change_position(
        token_count1: int,
        token_count2: int,
        i1: int,
        i2: int,
        j1: int,
        j2: int,
    ) -> str:
        starts_near_beginning = min(i1, j1) <= 3
        ends_near_end = (
            token_count1 - i2 <= 2
            and token_count2 - j2 <= 2
        )
        if starts_near_beginning and ends_near_end:
            return "whole_block"
        if starts_near_beginning:
            return "start"
        if ends_near_end:
            return "end"
        return "middle"

    @staticmethod
    def _tokens(text: str) -> list[TextToken]:
        return [
            TextToken(match.group(0), match.start(), match.end())
            for match in _TOKEN_RE.finditer(text)
        ]

    @staticmethod
    def _changed_text(
        text: str,
        tokens: list[TextToken],
        start: int,
        end: int,
    ) -> str:
        if start >= end or not tokens:
            return ""
        return text[tokens[start].start : tokens[end - 1].end]

    @classmethod
    def _context(
        cls,
        text: str,
        tokens: list[TextToken],
        start: int,
        end: int,
        *,
        radius: int,
        max_chars: int,
    ) -> str:
        if not text or not tokens:
            return ""

        left_index = max(0, min(start, len(tokens)) - radius)
        anchor = min(start, len(tokens) - 1)
        right_anchor = max(anchor + 1, min(end, len(tokens)))
        right_index = min(len(tokens), right_anchor + radius)
        excerpt = text[tokens[left_index].start : tokens[right_index - 1].end]
        excerpt = re.sub(r"\s+", " ", excerpt).strip()
        if len(excerpt) <= max_chars:
            return excerpt

        changed = cls._changed_text(text, tokens, start, end)
        changed = re.sub(r"\s+", " ", changed).strip()
        position = excerpt.find(changed) if changed else len(excerpt) // 2
        if position < 0:
            position = len(excerpt) // 2
        center = position + len(changed) // 2
        window_start = max(0, center - max_chars // 2)
        window_end = min(len(excerpt), window_start + max_chars)
        window_start = max(0, window_end - max_chars)
        clipped = excerpt[window_start:window_end].strip()
        if window_start > 0:
            clipped = f"…{clipped[1:]}" if clipped else "…"
        if window_end < len(excerpt):
            clipped = f"{clipped[:-1]}…" if clipped else "…"
        return clipped

    @staticmethod
    def _clip(text: str, max_chars: int) -> str:
        text = re.sub(r"\s+", " ", text).strip()
        if len(text) <= max_chars:
            return text
        return f"{text[: max_chars - 1].rstrip()}…"

    @staticmethod
    def _change_tags(file1: str, file2: str) -> tuple[str, ...]:
        tags = {"literal"}
        combined = f"{file1}{file2}"
        if any(char.isalpha() for char in combined):
            tags.add("word")
        if any(char.isdigit() for char in combined):
            tags.add("digit")
        if any(not char.isalnum() and not char.isspace() for char in combined):
            tags.add("punctuation")
        if file1 != file2 and file1.casefold() == file2.casefold():
            tags.add("case")

        left = collapse_whitespace(file1).casefold().replace(" ", "")
        right = collapse_whitespace(file2).casefold().replace(" ", "")
        if (
            left.startswith("не") and left[2:] == right
        ) or (
            right.startswith("не") and right[2:] == left
        ):
            tags.add("prefix_ne")
        if (
            left.startswith("без") and left[3:] == right
        ) or (
            right.startswith("без") and right[3:] == left
        ):
            tags.add("prefix_bez")
        return tuple(sorted(tags))

