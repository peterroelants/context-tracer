import pprint
from datetime import datetime, timedelta
from io import StringIO

from .time_utils import format_timedelta


class NewLineStrPrettyPrinter(pprint.PrettyPrinter):
    """
    PrettyPrinter that prints strings with newlines as triple-quoted strings.

    Improved dictionary formatting more similar to json.dumps.

    Based in part on `pprint.PrettyPrinter._safe_repr`.
    """

    # Override pprint.PrettyPrinter.format
    def format(self, object, context, maxlevels, level) -> tuple[str, bool, bool]:
        print(f"NewLineStrPrettyPrinter.format({type(object)=}, {object=})")
        # String formatting
        if isinstance(object, str):
            # Print strings with newlines as triple-quoted strings.
            return repr_multiline_str(object), True, False
        # datetime formatting
        if isinstance(object, datetime):
            return object.isoformat(sep=" ", timespec="seconds"), True, False
        if isinstance(object, timedelta):
            return format_timedelta(object), True, False
        # Dictionary formatting more similar to json.dumps.
        if isinstance(object, dict):
            if not object:
                return "{}", True, False
            objid = id(object)
            if maxlevels and level >= maxlevels:
                return "{...}", False, objid in context
            if objid in context:
                return pprint._recursion(object), False, True  # type: ignore
            context[objid] = 1
            readable = True
            recursive = False
            components: list[str] = []
            append = components.append
            level += 1
            items = object.items()
            for k, v in items:
                krepr, kreadable, krecur = self.format(k, context, maxlevels, level)
                vrepr, vreadable, vrecur = self.format(v, context, maxlevels, level)
                append(f"{krepr}: {vrepr.lstrip()}")
                readable = readable and kreadable and vreadable
                if krecur or vrecur:
                    recursive = True
            del context[objid]
            # Indent similar to json.dumps
            indent_per_level = self._indent_per_level  # type: ignore
            ident_repr = " " * (indent_per_level * level)
            comp_repr = ident_repr + f",\n{ident_repr}".join(components)
            ident_repr_prev_level = " " * (indent_per_level * (level - 1))
            dct_repr = (
                ident_repr_prev_level
                + "{\n"
                + comp_repr
                + "\n"
                + ident_repr_prev_level
                + "}"
            )
            return dct_repr.strip(), readable, recursive
        return super().format(object, context, maxlevels, level)

    # Override pprint.PrettyPrinter._format
    def _format(self, object, stream, indent, allowance, context, level):
        # Avoid the max_width check in pprint.PrettyPrinter._format
        objid = id(object)
        if objid in context:
            stream.write(pprint._recursion(object))
            self._recursive = True
            self._readable = False
            return
        rep = self._repr(object, context, level)
        stream.write(rep)

    def pformat_with_indent(self, object, level: int = 0, indent: int = 2) -> str:
        sio = StringIO()
        self._format(object, sio, indent, 0, {}, level)
        return sio.getvalue()


def repr_multiline_str(s: str) -> str:
    """Format a multiline string with triple quotes."""
    if "\n" in s:
        return f'"""\n{s!s}\n"""'
    return repr(s)
