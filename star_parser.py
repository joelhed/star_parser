#!/usr/bin/env python
"""Generic STAR file parser."""
from typing import Dict
import argparse
import pandas


StarFile = Dict[str, pandas.DataFrame]


class StarParseError(Exception):
    """Error raised when we were unable to parse a STAR file."""

    def __init__(self, stream, descr, *args, **kwargs):
        self.filename = stream.fname
        self.row = stream.row
        self.description = descr
        message = f'{stream.fname}: {stream.row}: {descr}'
        super().__init__(message, *args, **kwargs)


class LineStream:
    __slots__ = ("fname", "_iter", "_curr")

    def __init__(self, file):
        self.fname = file.name
        self._iter = enumerate(iter(file))
        self._curr = None
        self.next()

    @property
    def row(self):
        """The current row number, starting from 1."""
        return self._curr[0] + 1

    @property
    def curr(self):
        """The current line."""
        return self._curr[1] if self._curr is not None else None

    def next(self):
        """Move to the next line"""
        try:
            self._curr = next(self._iter)
        except StopIteration:
            self._curr = None


class StarParser:
    """The STAR file parser."""

    def __init__(self, file):
        self.stream = LineStream(file)

    def accept(self, item):
        if self.stream.curr == item:
            self.stream.next()
            return True

        return False

    def expect(self, item):
        if not self.accept(item):
            raise StarParseError(f"expected '{item}', found '{self.stream.curr}'")

    def parse(self) -> StarFile:
        """Parse the STAR file."""
        # TODO: ignore comments
        # TODO: parse string blocks
        while self.accept("\n"):
            pass

        data_blocks = {}
        while True:
            data_block = self.parse_data_block()
            if data_block is None:
                break

            block_name, contents = data_block
            data_blocks[block_name] = contents

        return data_blocks

    def parse_data_block(self):
        data_tag = "data_"
        if self.stream.curr is None or not self.stream.curr.startswith(data_tag):
            return None

        block_name = self.stream.curr[len(data_tag):].strip()
        self.stream.next()
        self.accept("\n")
        # TODO: don't assume a data block only has one loop, there might be single key-value pairs too
        self.expect("loop_\n")
        loop_content = self.parse_loop_content()
        return block_name, loop_content

    def parse_loop_content(self):
        data_names = []
        while True:
            data_name = self.parse_data_name()
            if data_name is None:
                break
            data_names.append(data_name)

        if len(data_names) == 0:
            raise StarParseError("no data names found for loop")

        rows = []
        while not (self.accept("\n") or self.accept(None)):
            # TODO: handle strings (with spaces)
            # maybe using shlex?
            row = self.stream.curr.split()
            if len(row) != len(data_names):
                raise StarParseError("number of data items in row does not match number of data names")

            rows.append(row)
            self.stream.next()

        return pandas.DataFrame.from_records(rows, columns=data_names)

    def parse_data_name(self):
        if not self.stream.curr.startswith("_"):
            return None

        # TODO: look at the second value as well
        data_name = self.stream.curr[1:].split()[0]
        self.stream.next()
        return data_name


def parse_star(filename: str) -> StarFile:
    """Parse the star file with the given file name."""
    with open(filename) as f:
        file_contents = StarParser(f).parse()

    return file_contents
        

def main():
    arg_parser = argparse.ArgumentParser(description=__doc__)
    arg_parser.add_argument("infile", type=argparse.FileType())
    args = arg_parser.parse_args()

    star_parser = StarParser(args.infile)
    result = star_parser.parse()

    for block_name, contents in result.items():
        print("DATA BLOCK", block_name)
        print(contents)


if __name__ == "__main__":
    main()
