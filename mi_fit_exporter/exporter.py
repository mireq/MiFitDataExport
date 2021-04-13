# -*- coding: utf-8 -*-
import array
import glob
import html
import os
import shutil
import sqlite3
import subprocess
import sys
from bisect import bisect_left
from collections import namedtuple
from datetime import datetime
from itertools import accumulate


NO_VALUE = -2000000
FIX_BIP_GAPS = False


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
				time = datetime.utcfromtimestamp(point.time + track_data.start_time).isoformat()
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
			steptimes=array.array('l', [int((val or '0,0,0,0').split(',')[0]) for val in self.track_data.BULKGAIT.split(';')] if self.track_data.BULKGAIT else []),
			stride=array.array('l', [int((val or '0,0,0,0').split(',')[2]) for val in self.track_data.BULKGAIT.split(';')] if self.track_data.BULKGAIT else []),
			cadence=array.array('l', [int((val or '0,0,0,0').split(',')[3]) for val in self.track_data.BULKGAIT.split(';')] if self.track_data.BULKGAIT else []),
		)

	def interpolate_data(self, track_data):
		track_times = array.array('l', accumulate(track_data.times))
		hr_times = array.array('l', accumulate(track_data.hrtimes))
		step_times = array.array('l', accumulate(track_data.steptimes))

		def change_times(times, change, time_from):
			return array.array('l', (time + change if time >= time_from else time for time in times))

		times = list(sorted(set(track_times).union(hr_times).union(step_times)))

		if FIX_BIP_GAPS:
			# remove missing data (wtf?)
			time_to_trim = (times[-1] - track_data.cost_time) if track_times else 0
			while time_to_trim > 0:
				max_time = 0
				max_interval = 0
				last_time = 0
				for time in times:
					current_interval = time - last_time
					last_time = time
					if current_interval > max_interval:
						max_interval = current_interval
						max_time = time
				time_change = max(max_interval - time_to_trim, 1) - max_interval
				track_times = change_times(track_times, time_change, max_time)
				hr_times = change_times(hr_times, time_change, max_time)
				step_times = change_times(step_times, time_change, max_time)
				time_to_trim += time_change
				times = list(sorted(set(track_times).union(hr_times).union(step_times)))

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


class BaseExporter():
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
				self.export_track_row(row)
		except sqlite3.OperationalError:
			pass

	def export_track_row(self, row):
		raise NotImplementedError()


class GpxExporter(BaseExporter):
	def export_track_row(self, row):
		GpxFileExporter(self.output_directory, row).export()


class DebugExporter(BaseExporter):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.copy_static_files()

	def copy_static_files(self):
		try:
			shutil.copytree(
				os.path.join(os.path.dirname(__file__), 'html', 'static'),
				os.path.join(self.output_directory, 'static')
			)
		except FileExistsError:
			pass

	def export_track_row(self, row):
		writer = HtmlTableWriter(self.output_directory, os.path.join('tracks', f'track_{row.TRACKID}.html'))
		writer.set_title(str(row.TRACKID))
		writer.set_header('Time', 'Total time', 'Location', 'Altitude', 'Flags')

		times = [int(t) if t else 0 for t in row.BULKTIME.split(';')]
		locations = row.BULKLL.split(';')
		altitudes = row.BULKAL.split(';')
		flags = row.BULKFLAG.split(';')

		total_times = accumulate(times)
		locations = locations if len(locations) == len(times) else [''] * len(times)
		altitudes = altitudes if len(altitudes) == len(times) else [''] * len(times)
		flags = flags if len(flags) == len(times) else [''] * len(times)

		for table_row in zip(times, total_times, locations, altitudes, flags):
			writer.add_row(*table_row)
		writer.flush()


		if row.BULKHR:
			writer = HtmlTableWriter(self.output_directory, os.path.join('tracks', f'hr_{row.TRACKID}.html'))
			writer.set_title(str(row.TRACKID))
			writer.set_header('Time', 'Total time', 'HR')

			times = [int(t.split(',')[0] if t.split(',')[0] else 1) for t in row.BULKHR.split(';')]
			hr = [int(t.split(',')[1]) for t in row.BULKHR.split(';')]
			total_times = accumulate(times)

			for table_row in zip(times, total_times, hr):
				writer.add_row(*table_row)
			writer.flush()


class HtmlTableWriter():
	def __init__(self, output_directory, filename, template_name='base.html'):
		self.header = []
		self.rows = []
		self.title = ''
		self.output_directory = output_directory
		self.filename = filename
		with open(os.path.join(os.path.dirname(__file__), 'html', 'templates', template_name), 'r') as fp:
			self.html_template = fp.read()

	def set_title(self, title):
		self.title = title

	def set_header(self, *header):
		self.header = header

	def add_row(self, *row):
		self.rows.append(row)

	def flush(self):
		with open(os.path.join(self.output_directory, self.filename), 'w') as fp:
			css = ('../' * (len(self.filename.split(os.sep))-1)) + 'static/css/style.css';
			content = self.render_table()
			fp.write(self.html_template.format(css=css, content=content, title=self.title))

	def render_table(self):
		thead = self.render_header()
		tbody = self.render_body()
		return f'<table><thead><tr>{thead}</tr></thead><tbody>{tbody}</tbody></table>'

	def render_header(self):
		return ''.join('<th>%s</th>'%(html.escape(item)) for item in self.header)

	def render_body(self):
		return ''.join('<tr>%s</tr>'%(self.render_row(row)) for row in self.rows)

	def render_row(self, row):
		return ''.join('<td>%s</td>'%(html.escape(str(item))) for item in row)


class Exporter():
	output_dir = ''
	debug = False

	def make_path(self, dirname):
		return os.path.join(self.output_dir, dirname)

	def makedirs(self):
		os.makedirs(self.output_dir, exist_ok=True)
		os.makedirs(self.make_path('gpx'), exist_ok=True)
		os.makedirs(self.make_path('database'), exist_ok=True)
		if self.debug:
			os.makedirs(self.make_path('debug_html'), exist_ok=True)
			os.makedirs(self.make_path(os.path.join('debug_html', 'tracks')), exist_ok=True)

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
			self.make_path('database'),
			"--strip-components=1"
		])
		if output.returncode:
			sys.stderr.write("Failed to extract archive\n")
			sys.exit(-1)

	def export_gpx(self, output_dir_name):
		exporter = GpxExporter(self.make_path('database'), output_dir_name)
		exporter.export_all_tracks()

	def export_debug(self, output_dir_name):
		exporter = DebugExporter(self.make_path('database'), output_dir_name)
		exporter.export_all_tracks()

	def export(self):
		self.makedirs()
		self.make_archive()
		self.download_archive()
		self.extract_archive()
		self.export_gpx(self.make_path('gpx'))
		if self.debug:
			self.export_debug(self.make_path('debug_html'))
