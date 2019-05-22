#!/usr/bin/python3
# -*- coding: utf-8 -*-
import argparse

import mi_fit_exporter


def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("output_dir", help="Output directory")
	args = parser.parse_args()

	exporter = mi_fit_exporter.Exporter()
	exporter.output_dir = args.output_dir
	exporter.export()


if __name__ == "__main__":
	main()
