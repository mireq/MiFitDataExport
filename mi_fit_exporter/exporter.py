# -*- coding: utf-8 -*-
from collections import namedtuple
import array
import itertools
import glob
import os
import sqlite3
import subprocess
import sys
from datetime import datetime


TrackRow = namedtuple('TrackRow', ['id', 'location', 'altitude', 'hr', 'time'])
NO_VALUE = 2**21


def bulk_convert(column):
	if column is None:
		return None
	items = column.split(';')
	if len(items) == 0:
		return items
	data = [[int(num) if num else NO_VALUE for num in item.split(',')] for item in items]
	return [array.array('l', lst) for lst in zip(*data)]


def bulk_accumulate(data):
	return array.array('l', itertools.accumulate(data))


def array_add_constant(data, constant):
	return array.array('l', (item + constant for item in data))


class GpxExporter():
	def __init__(self, temp_directory, output_directory):
		self.temp_directory = temp_directory
		self.output_directory = output_directory

	def export_all_tracks(self):
		for dbfile in glob.glob(os.path.join(glob.escape(self.temp_directory), 'origin_db_' + ('[0123456789abcdef]' * 32))):
			self.export_all_tracks_from_dbfile(dbfile)

	def export_all_tracks_from_dbfile(self, dbfile):
		conn = sqlite3.connect(os.path.join(self.temp_directory, dbfile))
		self.export_all_tracks_from_database(conn)
		conn.close()

	def export_all_tracks_from_database(self, conn):
		for row in conn.execute('SELECT TRACKID, BULKLL, BULKAL, BULKHR, BULKTIME FROM trackdata'):
			track_id = row[0]
			location = bulk_convert(row[1])
			altitude = bulk_convert(row[2])
			heart_rate = bulk_convert(row[3])
			time = bulk_convert(row[4])
			if len(location) != 2:
				continue
			location[0] = bulk_accumulate(location[0])
			location[1] = bulk_accumulate(location[1])
			time = bulk_accumulate(time[0])
			altitude = altitude[0]
			heart_rate = self.fill_heart_rate(heart_rate, time)
			time = array_add_constant(time, track_id)
			self.export_track(TrackRow(track_id, location, altitude, heart_rate, time))

	def export_track(self, track):
		with open(os.path.join(self.output_directory, f'{track.id}.gpx'), 'w') as fp:
			time = datetime.utcfromtimestamp(track.time[0]).isoformat()
			fp.write('<?xml version="1.0" encoding="UTF-8" standalone="no" ?>')
			fp.write('<gpx xmlns="http://www.topografix.com/GPX/1/1" xmlns:gpxx="http://www.garmin.com/xmlschemas/GpxExtensions/v3" xmlns:gpxtpx="http://www.garmin.com/xmlschemas/TrackPointExtension/v1" creator="Oregon 400t" version="1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.topografix.com/GPX/1/1 http://www.topografix.com/GPX/1/1/gpx.xsd http://www.garmin.com/xmlschemas/GpxExtensions/v3 http://www.garmin.com/xmlschemas/GpxExtensionsv3.xsd http://www.garmin.com/xmlschemas/TrackPointExtension/v1 http://www.garmin.com/xmlschemas/TrackPointExtensionv1.xsd">')
			fp.write('<metadata>')
			fp.write(f'<time>{time}</time>')
			fp.write('</metadata>')
			fp.write('<trk>')
			fp.write(f'<name>{time}</name>')
			fp.write('<trkseg>')
			last_ele = 0
			for lat, lon, ele, time, hr in zip(track.location[0], track.location[1], track.altitude, track.time, track.hr):
				if ele == -2000000:
					ele = last_ele
				last_ele = ele
				lat = lat / 100000000
				lon = lon / 100000000
				ele = ele / 100
				time = datetime.utcfromtimestamp(time).isoformat()
				fp.write(f'\n<trkpt lat="{lat}" lon="{lon}">')
				fp.write(f'<ele>{ele}</ele>')
				fp.write(f'<time>{time}</time>')
				fp.write(f'<extensions><gpxtpx:TrackPointExtension><gpxtpx:hr>{hr}</gpxtpx:hr></gpxtpx:TrackPointExtension></extensions>')
				fp.write(f'</trkpt>')
			fp.write('\n</trkseg>')
			fp.write('</trk>')
			fp.write('</gpx>')

	def fill_heart_rate(self, hr_data, time_values):
		hr = 0
		hr_pointer = -1
		current_time = 0
		hr_values = []
		for time in time_values:
			while current_time < time:
				hr_pointer += 1
				if hr_pointer >= len(hr_data[0]):
					break
				if hr_data[0][hr_pointer] == NO_VALUE:
					current_time += 1
				else:
					current_time += hr_data[0][hr_pointer]
				hr += hr_data[1][hr_pointer]
			hr_values.append(hr)
		return array.array('l', hr_values)


class Exporter():
	temp_directory = '/tmp/mi_fit_export/'

	def make_archive(self):
		subprocess.run(["adb", "shell", "su -c 'cd /data/data/com.xiaomi.hm.health/&&tar -cf /sdcard/mi_fit.tar databases/'"])

	def download_archive(self):
		output = subprocess.run(["adb", "pull", "/sdcard/mi_fit.tar", "/tmp/mi_fit_export/"])
		if output.returncode:
			sys.stderr.write("Pull command failed, has device root access?")
			sys.exit(-1)
		subprocess.run(["adb", "shell", "rm /sdcard/mi_fit.tar"])

	def extract_archive(self):
		output = subprocess.run([
			"tar",
			"-xf",
			os.path.join(self.temp_directory, "mi_fit.tar"),
			"-C",
			self.temp_directory,
			"--strip-components=1"]
		)
		if output.returncode:
			sys.stderr.write("Failed to extract archive")
			sys.exit(-1)

	def export_gpx(self, dir_name):
		os.makedirs(dir_name, exist_ok=True)
		exporter = GpxExporter(self.temp_directory, dir_name)
		exporter.export_all_tracks()

	def export(self):
		#self.make_archive()
		#self.download_archive()
		#self.extract_archive()
		self.export_gpx('/tmp/mi_fit_export/gpx/')
