#!/usr/bin/python
# -*- coding: utf-8 -*-

import unittest
import controller
import StringIO
import sys

"""
class TestDeserializer(unittest.TestCase):
    def _test_deserialize(self, input_str, expected_output):
        self.assertEquals(controller.deserialize_input(input_str), expected_output)

    def test_no_params(self):
        self._test_deserialize("", [])

    def test_null_byte(self):
        self._test_deserialize("\x16", [None])

    def test_empty_charrarray(self):
        self._test_deserialize("C", [""])

    def test_chararray(self):
        self._test_deserialize("Camber", ["amber"])

    def test_unicode_chararray(self):
        self._test_deserialize("Cಠ_ಠ".encode('utf-8'), ["ಠ_ಠ".encode('utf-8')])

    def test_bytearray(self):
        self._test_deserialize("Aamber", [bytearray(b"amber")])

    def test_int(self):
        self._test_deserialize("I11235", [11235])

    def test_empty_int(self):
        self._test_deserialize("I", [None])

    def test_long(self):
        self._test_deserialize("L11235813213455", [11235813213455L])

    def test_float(self):
        self._test_deserialize("F3.1415", [3.1415])

    def test_double(self):
        self._test_deserialize("D3.14159265359", [3.14159265359])

    def test_boolean_true(self):
        self._test_deserialize("Btrue", [True])

    def test_boolean_false(self):
        self._test_deserialize("Bfalse", [False])

    def test_two_params(self):
        self._test_deserialize("Cabc\x1DI123", ["abc", 123])

    def test_three_params_one_null(self):
        self._test_deserialize("Cabc\x1D\x16\x1DI123", ["abc", None, 123])

    def test_empty_tuple(self):
        self._test_deserialize("\x11\x12", [()])

    def test_tuple(self):
        self._test_deserialize("\x11Camber\x1FCberyl\x1FCchrysoprase\x12",
                               [("amber", "beryl", "chrysoprase")])

    def test_nested_tuple(self):
        self._test_deserialize("\x11Camber\x1F\x11\x11Cberyl\x12\x1FCchrysoprase\x12\x1FCdiamond\x12",
                               [("amber", (("beryl",), "chrysoprase"), "diamond")])

    def test_empty_bag(self):
        self._test_deserialize("\x13\x14", [[]])

    def test_bag_of_single_element_tuples(self):
        self._test_deserialize("\x13\x11Camber\x12\x1F\x11Cberyl\x12\x1F\x11Cchrysoprase\x12\x14",
                               [[("amber",), ("beryl",), ("chrysoprase",)]])

    def test_bag_of_two_element_tuples(self):
        input_str = "\x13\x11Camber\x1FCberyl\x12\x1F\x11Cchrysoprase\x1FCdiamond\x12\x1F\x11Cemerald\x1FCfeldspar\x12\x14"
        expected_output = [[("amber", "beryl"), ("chrysoprase", "diamond"), ("emerald", "feldspar")]]
        self._test_deserialize(input_str, expected_output)

    def test_bag_of_bags(self):
        input_str = "\x13\x13\x11Camber\x12\x1F\x11Cberyl\x12\x14\x1F\x13\x11Cchrysoprase\x12\x1F\x11Cdiamond\x12\x14\x14"
        expected_output = [[[("amber",), ("beryl",)], [("chrysoprase",), ("diamond",)]]]
        self._test_deserialize(input_str, expected_output)

    def test_empty_map(self):
        self._test_deserialize("\x05\x06", [{}])

    def test_map(self):
        input_str = "\x05Camber\x1ACberyl\x1FCchrysoprase\x1ACdiamond\x1FCemerald\x1ACfeldspar\x06"
        expected_output = [{"amber": "beryl", "chrysoprase": "diamond", "emerald": "feldspar"}]
        self._test_deserialize(input_str, expected_output)

    def test_map_with_null_values(self):
        self._test_deserialize("\x05Camber\x1A\x16\x1FCberyl\x1ACchrysoprase\x06",
                               [{"amber": None, "beryl": "chrysoprase"}])

    def test_map_to_tuples(self):
        input_str = "\x05Camber\x1A\x11Cberyl\x1FCchrysoprase\x12\x1FCdiamond\x1A\x11Cemerald\x1FCfeldspar\x12\x06"
        expected_output = [{"amber": ("beryl", "chrysoprase"), "diamond": ("emerald", "feldspar")}]
        self._test_deserialize(input_str, expected_output)

    def test_map_to_maps(self):
        input_str = "\x05Camber\x1A\x05Cberyl\x1ACchrysoprase\x1FCdiamond\x1ACemerald\x06\x1FCfeldspar\x1A\x05Cgarnet\x1AChematite\x1FCjade\x1ACkornerupine\x06\x06"
        expected_output = [{"amber": {"beryl": "chrysoprase", "diamond": "emerald"}, "feldspar": {"garnet": "hematite", "jade": "kornerupine"}}]
        self._test_deserialize(input_str, expected_output)

    def test_potluck(self):
        input_str = "I1\x1D\x11L2\x1F\x05Camber\x1AF1.0\x1FCberyl\x1A\x13\x11Btrue\x12\x1F\x11Bfalse\x12\x1F\x11Achrysoprase\x12\x1F\x11\x16\x12\x14\x06\x1FBfalse\x12\x1DD2.0"
        expected_output = [1, (2L, {"amber": 1.0, "beryl": [(True,), (False,), (bytearray(b"chrysoprase"),), (None,)]}, False), 2.0]
        self._test_deserialize(input_str, expected_output)

    def test_invalid_type(self):
        input_str = "K123"
        self.assertRaises(Exception, controller.deserialize_input, [input_str])

class TestSerializeOutput(unittest.TestCase):
    def _test_serialize(self, input_obj, expected_output):
        self.assertEquals(controller.serialize_output(input_obj), expected_output)

    def test_null(self):
        self._test_serialize(None, "\x16")

    def test_empty_charrarray(self):
        self._test_serialize("", "")

    def test_chararray(self):
        self._test_serialize("amber", "amber")

    def test_unicode_chararray(self):
        self._test_serialize("ಠ_ಠ", "ಠ_ಠ".encode("utf-8"))

    def test_bytearray(self):
        self._test_serialize(bytearray(b"amber"), "amber")

    def test_int(self):
        self._test_serialize(11235, "11235")

    def test_long(self):
        self._test_serialize(11235813213455L, "11235813213455")

    def test_float(self):
        self._test_serialize(3.1415, "3.1415")

    def test_double(self):
        self._test_serialize(3.14159265359, "3.14159265359")

    def test_boolean_true(self):
        self._test_serialize(True, "1")

    def test_boolean_false(self):
        self._test_serialize(False, "0")

    def test_empty_tuple(self):
        self._test_serialize(tuple(), "\x11\x12")

    def test_tuple(self):
        self._test_serialize(("amber", "beryl", "chrysoprase"),
                             "\x11amber\x1Fberyl\x1Fchrysoprase\x12")

    def test_nested_tuple(self):
        self._test_serialize(("amber", (("beryl",), "chrysoprase"), "diamond"),
                             "\x11amber\x1F\x11\x11beryl\x12\x1Fchrysoprase\x12\x1Fdiamond\x12")

    def test_empty_bag(self):
        self._test_serialize([], "\x13\x14")

    def test_bag_of_single_element_tuples(self):
        self._test_serialize([("amber",), ("beryl",), ("chrysoprase",)],
                              "\x13\x11amber\x12\x1F\x11beryl\x12\x1F\x11chrysoprase\x12\x14")

    def test_bag_of_two_element_tuples(self):
        input_obj = [("amber", "beryl"), ("chrysoprase", "diamond"), ("emerald", "feldspar")]
        expected_output = "\x13\x11amber\x1Fberyl\x12\x1F\x11chrysoprase\x1Fdiamond\x12\x1F\x11emerald\x1Ffeldspar\x12\x14"
        self._test_serialize(input_obj, expected_output)

    def test_bag_of_bags(self):
        input_obj = [[("amber",), ("beryl",)], [("chrysoprase",), ("diamond",)]]
        expected_output = "\x13\x13\x11amber\x12\x1F\x11beryl\x12\x14\x1F\x13\x11chrysoprase\x12\x1F\x11diamond\x12\x14\x14"
        self._test_serialize(input_obj, expected_output)

    def test_empty_map(self):
        self._test_serialize({}, "\x05\x06")

    def test_map(self):
        input_obj = {"amber": "beryl", "chrysoprase": "diamond", "emerald": "feldspar"}
        expected_output = "\x05amber\x1Aberyl\x1Fchrysoprase\x1Adiamond\x1Femerald\x1Afeldspar\x06"
        self._test_serialize(input_obj, expected_output)

    def test_map_to_tuples(self):
        input_obj = {"amber": ("beryl", "chrysoprase"), "diamond": ("emerald", "feldspar")}
        expected_output = "\x05amber\x1A\x11beryl\x1Fchrysoprase\x12\x1Fdiamond\x1A\x11emerald\x1Ffeldspar\x12\x06"
        self._test_serialize(input_obj, expected_output)

    def test_map_to_maps(self):
        input_obj = {"amber": {"beryl": "chrysoprase", "diamond": "emerald"}, "feldspar": {"garnet": "hematite", "jade": "kornerupine"}}
        expected_output = "\x05amber\x1A\x05beryl\x1Achrysoprase\x1Fdiamond\x1Aemerald\x06\x1Ffeldspar\x1A\x05garnet\x1Ahematite\x1Fjade\x1Akornerupine\x06\x06"
        self._test_serialize(input_obj, expected_output)

    def test_potluck(self):
        input_obj = (2L, {"amber": 1.0, "beryl": [(True,), (False,), (bytearray(b"chrysoprase"),), (None,)]}, False)
        expected_output = "\x112\x1F\x05amber\x1A1.0\x1Fberyl\x1A\x13\x111\x12\x1F\x110\x12\x1F\x11chrysoprase\x12\x1F\x11\x16\x12\x14\x06\x1F0\x12"
        self._test_serialize(input_obj, expected_output)

class TestReadInput(unittest.TestCase):
    def test_multiline_record(self):
        input_io = StringIO.StringIO()
        input_io.write('12\n')
        input_io.write('34\n')
        input_io.write('5\x1E\n')
        input_io.seek(0)

        test_controller = controller.PythonStreamingController()
        test_controller.input_stream = input_io
        test_controller.output_stream = sys.stdout

        out = test_controller.get_next_input()
        self.assertEquals('12\n34\n5', out)

    def test_complex_multiline_record(self):
        input_io = StringIO.StringIO()
        input_io.write('\x13\x1132,12,a\n')
        input_io.write('bc\x12,32,\x13ab\n')
        input_io.write('c,def,gh\n')
        input_io.write('i\x14\x14\x1E\n')
        input_io.seek(0)

        test_controller = controller.PythonStreamingController()
        test_controller.input_stream = input_io
        test_controller.output_stream = sys.stdout

        out = test_controller.get_next_input()
        self.assertEquals('\x13\x1132,12,a\nbc\x12,32,\x13ab\nc,def,gh\ni\x14\x14', out)
"""
