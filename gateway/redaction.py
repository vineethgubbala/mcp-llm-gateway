import re
import string

REDACTED = "[REDACTED]"
EMAIL_CHARS = frozenset(string.ascii_letters + string.digits + ".!#$%&'*+/=?^_`{|}~@-")
EMAIL = re.compile(r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+")
SSN = re.compile(r"(?<![0-9])[0-9]{3}-[0-9]{2}-[0-9]{4}(?![0-9])")


class StreamRedactor:
    """Two bounded stages: email atoms, then numeric runs including separators."""

    def __init__(self):
        self.atom = ""
        self.number = ""
        self.skip_atom = False
        self.skip_number = False

    def _finish_number(self):
        value, self.number = self.number, ""
        core = value.rstrip(" -")
        suffix = value[len(core) :]
        digits = sum(c in string.digits for c in core)
        if digits >= 13:
            return REDACTED + suffix
        return SSN.sub(REDACTED, value)

    def _numbers(self, text):
        out = []
        for char in text:
            if self.skip_number:
                if char in string.digits + " -":
                    continue
                self.skip_number = False
            if char in string.digits or (self.number and char in " -"):
                self.number += char
                if len(self.number) > 64:
                    out.append(REDACTED)
                    self.number = ""
                    self.skip_number = True
            else:
                out.append(self._finish_number())
                out.append(char)
        return "".join(out)

    def feed(self, text: str) -> str:
        out = []
        for char in text:
            if self.skip_atom:
                if char in EMAIL_CHARS:
                    continue
                self.skip_atom = False
            if char in EMAIL_CHARS:
                self.atom += char
                if len(self.atom) > 256:
                    out.append(self._numbers(REDACTED))
                    self.atom = ""
                    self.skip_atom = True
            else:
                out.append(self._numbers(EMAIL.sub(REDACTED, self.atom) + char))
                self.atom = ""
        return "".join(out)

    def finish(self) -> str:
        result = self._numbers(EMAIL.sub(REDACTED, self.atom)) + self._finish_number()
        self.atom = ""
        self.skip_atom = self.skip_number = False
        return result
