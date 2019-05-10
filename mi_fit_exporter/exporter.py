# -*- coding: utf-8 -*-
import array
import glob
import os
import sqlite3
import subprocess
import sys
from bisect import bisect_left
from collections import namedtuple
from datetime import datetime
from itertools import accumulate


NO_VALUE = -2000000


RawTrackData = namedtuple('RawTrackData', ['start_time', 'end_time', 'cost_time', 'distance', 'times', 'lat', 'lon', 'alt', 'hrtimes', 'hr', 'steptimes', 'stride', 'cadence'])
Position = namedtuple('Position', ['lat', 'lon', 'alt'])
TrackPoint = namedtuple('TrackPoint', ['time', 'position', 'hr', 'stride', 'cadence'])


class Interpolate(object):
	def __init__(self, x_list, y_list):
		intervals = zip(x_list, x_list[1:], y_list, y_list[1:])
		self.x_list = x_list
		self.y_list = y_list
		self.slopes = [(y2 - y1)//((x2 - x1) or 1) for x1, x2, y1, y2 in intervals]

	def __getitem__(self, x):
		i = bisect_left(self.x_list, x) - 1
		if i >= len(self.slopes):
			return self.y_list[-1]
		if i < 0:
			return self.y_list[0]
		return self.y_list[i] + self.slopes[i] * (x - self.x_list[i])


class GpxFileExporter():
	def __init__(self, output_directory, track_data):
		self.output_directory = output_directory
		self.track_data = track_data

	def export(self):
		track_file = os.path.join(self.output_directory, f'{self.track_data.TRACKID}.gpx')
		track_data = self.parse_track_data()
		if not track_data.lat:
			return
		if os.path.exists(track_file):
			return
		ind = '\t'
		with open(track_file, 'w') as fp:
			time = datetime.utcfromtimestamp(track_data.start_time).isoformat()
			fp.write('<?xml version="1.0" encoding="UTF-8" standalone="no" ?>\n')
			fp.write('<gpx xmlns="http://www.topografix.com/GPX/1/1" xmlns:gpxdata="http://www.cluetrust.com/XML/GPXDATA/1/0" xmlns:gpxtpx="http://www.garmin.com/xmlschemas/TrackPointExtension/v1">\n')
			fp.write(f'{ind}<metadata><time>{time}</time></metadata>\n')
			fp.write(f'{ind}<trk>\n')
			fp.write(f'{ind}{ind}<name>{time}</name>\n')
			fp.write(f'{ind}{ind}<trkseg>\n')
			for point in self.track_points(self.interpolate_data(track_data)):
				time = datetime.utcfromtimestamp(point.time).isoformat()
				ext_hr = ''
				ext_cadence = ''
				if point.hr:
					ext_hr = f'<gpxtpx:TrackPointExtension><gpxtpx:hr>{point.hr}</gpxtpx:hr></gpxtpx:TrackPointExtension><gpxdata:hr>{point.hr}</gpxdata:hr>'
				if point.cadence:
					ext_cadence = f'<gpxdata:cadence>{point.cadence}</gpxdata:cadence>'
				fp.write(
					f'{ind}{ind}{ind}<trkpt lat="{point.position.lat}" lon="{point.position.lon}">'
					f'<ele>{point.position.alt}</ele>'
					f'<time>{time}</time>'
					f'<extensions>'
					f'{ext_hr}{ext_cadence}'
					f'</extensions>'
					f'</trkpt>\n'
				)
			fp.write(f'{ind}{ind}</trkseg>\n')
			fp.write(f'{ind}</trk>\n')
			fp.write('</gpx>')

	def parse_track_data(self):
		return RawTrackData(
			start_time=int(self.track_data.TRACKID),
			end_time=int(self.track_data.ENDTIME),
			cost_time=int(self.track_data.COSTTIME),
			distance=int(self.track_data.DISTANCE),
			times=array.array('l', [int(val) for val in self.track_data.BULKTIME.split(';')] if self.track_data.BULKTIME else []),
			lat=array.array('l', [int(val.split(',')[0]) for val in self.track_data.BULKLL.split(';')] if self.track_data.BULKLL else []),
			lon=array.array('l', [int(val.split(',')[1]) for val in self.track_data.BULKLL.split(';')] if self.track_data.BULKLL else []),
			alt=array.array('l', [int(val) for val in self.track_data.BULKAL.split(';')] if self.track_data.BULKAL else []),
			hrtimes=array.array('l', [int(val.split(',')[0] or 1) for val in self.track_data.BULKHR.split(';')] if self.track_data.BULKHR else []),
			hr=array.array('l', [int(val.split(',')[1]) for val in self.track_data.BULKHR.split(';')] if self.track_data.BULKHR else []),
			steptimes=array.array('l', [int(val.split(',')[0]) for val in self.track_data.BULKGAIT.split(';')] if self.track_data.BULKGAIT else []),
			stride=array.array('l', [int(val.split(',')[2]) for val in self.track_data.BULKGAIT.split(';')] if self.track_data.BULKGAIT else []),
			cadence=array.array('l', [int(val.split(',')[3]) for val in self.track_data.BULKGAIT.split(';')] if self.track_data.BULKGAIT else []),
		)

	def interpolate_data(self, track_data):
		times = list(sorted(set(accumulate(track_data.times)).union(accumulate(track_data.hrtimes)).union(accumulate(track_data.steptimes))))
		track_times = array.array('l', accumulate(track_data.times))
		hr_times = array.array('l', accumulate(track_data.hrtimes))
		step_times = array.array('l', accumulate(track_data.steptimes))

		return track_data._replace(
			times=times,
			lat=self.interpolate_column(accumulate(track_data.lat), track_times, times),
			lon=self.interpolate_column(accumulate(track_data.lon), track_times, times),
			alt=self.interpolate_column(track_data.alt, track_times, times),
			hrtimes=times,
			hr=self.interpolate_column(accumulate(track_data.hr), hr_times, times),
			steptimes=times,
			stride=self.interpolate_column(track_data.stride, step_times, times),
			cadence=self.interpolate_column(track_data.cadence, step_times, times),
		)

	def interpolate_column(self, data, original_points, new_points):
		# fill gaps
		data = array.array('l', data)
		old_value = NO_VALUE
		for old_value in data:
			if old_value != NO_VALUE:
				break
		for i, value in enumerate(data):
			if value == NO_VALUE:
				data[i] = old_value
			else:
				old_value = value

		if len(new_points) == 0:
			return array.array('l', [])
		if len(original_points) == 0:
			return array.array('l', [0] * len(new_points))
		if len(original_points) == 1:
			return array.array('l', [original_points[1]] * len(new_points))
		interpolate = Interpolate(original_points, data)
		return array.array('l', (interpolate[point] for point in new_points))

	def track_points(self, track_data):
		for time, lat, lon, alt, hr, stride, cadence in zip(track_data.times, track_data.lat, track_data.lon, track_data.alt, track_data.hr, track_data.stride, track_data.cadence):
			yield TrackPoint(
				time=time,
				position=Position(lat=lat / 100000000, lon=lon / 100000000, alt=alt / 100),
				hr=hr,
				stride=stride,
				cadence=cadence,
			)


class GpxExporter():
	def __init__(self, input_directory, output_directory):
		self.input_directory = input_directory
		self.output_directory = output_directory

	def export_all_tracks(self):
		for dbfile in glob.glob(os.path.join(glob.escape(self.input_directory), 'origin_db_' + ('[0123456789abcdef]' * 32))):
			self.export_all_tracks_from_dbfile(dbfile)

	def export_all_tracks_from_dbfile(self, dbfile):
		conn = sqlite3.connect(dbfile)
		self.export_all_tracks_from_database(conn)
		conn.close()

	def export_all_tracks_from_database(self, conn):
		columns = (
			'TRACKDATA.TRACKID',
			'TRACKDATA.BULKLL',
			'TRACKDATA.BULKGAIT',
			'TRACKDATA.BULKAL',
			'TRACKDATA.BULKTIME',
			'TRACKDATA.BULKHR',
			'TRACKDATA.BULKPAUSE',
			'TRACKDATA.BULKSPEED',
			'TRACKDATA.TYPE',
			'TRACKDATA.BULKFLAG',
			'TRACKRECORD.COSTTIME',
			'TRACKRECORD.ENDTIME',
			'TRACKRECORD.DISTANCE',
		)
		sql = """SELECT
			{columns}
			FROM TRACKDATA, TRACKRECORD
			WHERE TRACKDATA.TRACKID = TRACKRECORD.TRACKID
			ORDER BY TRACKDATA.TRACKID""".format(columns=', '.join(columns))
		RowRecord = namedtuple('RowRecord', (col.split('.')[-1] for col in columns))
		try:
			for row in conn.execute(sql):
				row = RowRecord(*row)
				GpxFileExporter(self.output_directory, row).export()
		except sqlite3.OperationalError:
			pass


class Exporter():
	output_dir = ''

	def makedirs(self):
		os.makedirs(self.output_dir, exist_ok=True)
		os.makedirs(os.path.join(self.output_dir, 'gpx'), exist_ok=True)
		os.makedirs(os.path.join(self.output_dir, 'database'), exist_ok=True)

	def make_archive(self):
		output = subprocess.run([
			"adb",
			"shell",
			"su -c 'cd /data/data/com.xiaomi.hm.health/&&tar -cf /sdcard/mi_fit.tar databases/'"
		])
		if output.returncode:
			sys.stderr.write("Archive command failed\n")

	def download_archive(self):
		output_file = os.path.join(self.output_dir, 'database', 'mi_fit.tar')
		output = subprocess.run([
			"adb",
			"pull",
			"/sdcard/mi_fit.tar",
			output_file
		])
		if output.returncode:
			sys.stderr.write("Pull command failed, has device root access?\n")
		subprocess.run([
			"adb",
			"shell",
			"rm /sdcard/mi_fit.tar"
		])

	def extract_archive(self):
		output_file = os.path.join(self.output_dir, 'database', 'mi_fit.tar')
		if not os.path.exists(output_file):
			sys.stderr.write("mi_fit.tar not downloaded\n")
			sys.exit(-1)
		output = subprocess.run([
			"tar",
			"-xf",
			output_file,
			"-C",
			os.path.join(self.output_dir, 'database'),
			"--strip-components=1"
		])
		if output.returncode:
			sys.stderr.write("Failed to extract archive\n")
			sys.exit(-1)

	def export_gpx(self, dir_name):
		exporter = GpxExporter(os.path.join(self.output_dir, 'database'), dir_name)
		exporter.export_all_tracks()

	def export(self):
		self.makedirs()
		self.make_archive()
		self.download_archive()
		self.extract_archive()
		self.export_gpx(os.path.join(self.output_dir, 'gpx'))
