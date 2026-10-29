import random

import pytest

from gateway.redaction import StreamRedactor


def redact(chunks):
    guard = StreamRedactor()
    output = []
    for chunk in chunks:
        output.append(guard.feed(chunk))
        assert len(guard.atom) <= 256
        assert len(guard.number) <= 64
    return "".join(output) + guard.finish()


@pytest.mark.parametrize(
    "text,expected",
    [
        ("Email alice.smith+test@example.co.uk!", "Email [REDACTED]!"),
        ("SSN 123-45-6789.", "SSN [REDACTED]."),
        ("Card 4111 1111 1111 1111.", "Card [REDACTED]."),
        ("Card 4111-1111-1111-1111.", "Card [REDACTED]."),
        ("4111111111111111", "[REDACTED]"),
        ("123-45-6789 12", "[REDACTED] 12"),
        ("4111111111111111 12345", "[REDACTED]"),
        ("Price 12.50, order 12345.", "Price 12.50, order 12345."),
        ("Hi 🌱 hello café", "Hi 🌱 hello café"),
        ("a@b.com and c@d.org", "[REDACTED] and [REDACTED]"),
    ],
)
def test_every_chunk_boundary(text, expected):
    assert redact([text]) == expected
    assert redact(list(text)) == expected
    for split in range(len(text) + 1):
        assert redact([text[:split], text[split:]]) == expected


def test_random_partitions():
    text = "Contact a@example.com. SSN 123-45-6789; card 4111 1111 1111 1111. Thanks."
    expected = "Contact [REDACTED]. SSN [REDACTED]; card [REDACTED]. Thanks."
    rng = random.Random(42)
    for _ in range(100):
        cuts = sorted({0, len(text), *(rng.randrange(len(text)) for _ in range(20))})
        assert redact([text[a:b] for a, b in zip(cuts, cuts[1:], strict=False)]) == expected


def test_ordinary_text_emits_before_end():
    guard = StreamRedactor()
    assert guard.feed("Hello ") == "Hello "
    assert guard.feed("alice@") == ""
    assert guard.feed("example.com ") == "[REDACTED] "


def test_overlong_candidate_fails_closed_with_bounded_state():
    assert redact(["x" * 128 for _ in range(1000)] + ["@example.com done"]) == "[REDACTED] done"
    assert redact(["1 " * 1000, "end"]) == "[REDACTED]end"
