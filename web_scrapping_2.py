# extract past reddit posts from india with dump files

# get the list of india cities

city = ['indian', 'kerala',
 'roorkee',
 'nalgonda',
 'harihar',
 'makrana',
 'shantipur',
 'dhuburi',
 'karauli',
 'hindupur',
 'amravati',
 'mainpuri',
 'jalna',
 'jalpaiguri',
 'dhar',
 'jalgaon',
 'odisha',
 'najibabad',
 'telangana',
 'hassan',
 'gujarat',
 'fatehpur',
 'muzaffarnagar',
 'sahaswan',
 'hardoi',
 'rourkela',
 'delhi',
 'india']

for i in range(len(city)):
  city.append(city[i].capitalize())
  city.append(city[i].upper())
city

# web scraping using dump files and filter based on matching of india locations

import zstandard
import os
import json
import sys
import csv
from datetime import datetime
import logging.handlers


def write_line_zst(handle, line):
	handle.write(line.encode('utf-8'))
	handle.write("\n".encode('utf-8'))


def write_line_json(handle, obj):
	handle.write(json.dumps(obj))
	handle.write("\n")


def write_line_single(handle, obj, field):
	if field in obj:
		handle.write(obj[field])
	else:
		log.info(f"{field} not in object {obj['id']}")
	handle.write("\n")


def write_line_csv(writer, obj, is_submission):
	output_list = []
	output_list.append(str(obj['score']))
	output_list.append(datetime.fromtimestamp(int(obj['created_utc'])).strftime("%Y-%m-%d"))
	if is_submission:
		output_list.append(obj['title'])
	output_list.append(f"u/{obj['author']}")
	output_list.append(f"https://www.reddit.com{obj['permalink']}")
	if is_submission:
		if obj['is_self']:
			if 'selftext' in obj:
				output_list.append(obj['selftext'])
			else:
				output_list.append("")
		else:
			output_list.append(obj['url'])
	else:
		output_list.append(obj['body'])
	writer.writerow(output_list)


def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
	chunk = reader.read(chunk_size)
	bytes_read += chunk_size
	if previous_chunk is not None:
		chunk = previous_chunk + chunk
	try:
		return chunk.decode()
	except UnicodeDecodeError:
		if bytes_read > max_window_size:
			raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
		log.info(f"Decoding error with {bytes_read:,} bytes, reading another chunk")
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_lines_zst(file_name):
	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)

			if not chunk:
				break
			lines = (buffer + chunk).split("\n")

			for line in lines[:-1]:
				yield line.strip(), file_handle.tell()

			buffer = lines[-1]

		reader.close()


def process_file(input_file, output_file, output_format, field, values, from_date, to_date, single_field, exact_match):
	output_path = f"{output_file}.{output_format}"
	is_submission = "submission" in input_file
	log.info(f"Input: {input_file} : Output: {output_path} : Is submission {is_submission}")
	writer = None
	if output_format == "zst":
		handle = zstandard.ZstdCompressor().stream_writer(open(output_path, 'wb'))
	elif output_format == "txt":
		handle = open(output_path, 'w', encoding='UTF-8')
	elif output_format == "csv":
		handle = open(output_path, 'w', encoding='UTF-8', newline='')
		writer = csv.writer(handle)
	else:
		log.error(f"Unsupported output format {output_format}")
		sys.exit()

	file_size = os.stat(input_file).st_size
	created = None
	matched_lines = 0
	bad_lines = 0
	total_lines = 0
	for line, file_bytes_processed in read_lines_zst(input_file):
		total_lines += 1
		if total_lines % 100000 == 0:
			log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {total_lines:,} : {matched_lines:,} : {bad_lines:,} : {file_bytes_processed:,}:{(file_bytes_processed / file_size) * 100:.0f}%")

		try:
			obj = json.loads(line)
			created = datetime.utcfromtimestamp(int(obj['created_utc']))

			if created < from_date:
				continue
			if created > to_date:
				continue

			if field is not None:
				field_value = obj[field].lower()
				matched = False
				for value in values:
					if exact_match:
						if value == field_value:
							matched = True
							break
					else:
						if value in field_value:
							matched = True
							break
				if not matched:
					continue

			matched_lines += 1
			if output_format == "zst":
				write_line_zst(handle, line)
			elif output_format == "csv":
				write_line_csv(writer, obj, is_submission)
			elif output_format == "txt":
				if single_field is not None:
					write_line_single(handle, obj, single_field)
				else:
					write_line_json(handle, obj)
			else:
				log.info(f"Something went wrong, invalid output format {output_format}")
		except (KeyError, json.JSONDecodeError) as err:
			bad_lines += 1
			if write_bad_lines:
				if isinstance(err, KeyError):
					log.warning(f"Key {field} is not in the object: {err}")
				elif isinstance(err, json.JSONDecodeError):
					log.warning(f"Line decoding failed: {err}")
				log.warning(line)

	handle.close()
	log.info(f"Complete : {total_lines:,} : {matched_lines:,} : {bad_lines:,}")


for i in ['Coronavirus', 'COVID', 'Health', 'india', 'medicine', 'publichealth', 'worldnews']:
		input_file = i + "_submissions.zst"
		i =i.lower()
		output_file = i + "_submissions_1"

		output_format = "csv"

		single_field = None

		write_bad_lines = True


		from_date = datetime.strptime("2015-01-01", "%Y-%m-%d")
		to_date = datetime.strptime("2023-12-31", "%Y-%m-%d")


		field = 'title'
		values = city

		values_file = None
		exact_match = False



		log = logging.getLogger("bot")
		log.setLevel(logging.INFO)
		log_formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
		log_str_handler = logging.StreamHandler()
		log_str_handler.setFormatter(log_formatter)
		log.addHandler(log_str_handler)

		if __name__ == "__main__":
			if single_field is not None:
				log.info("Single field output mode, changing output file format to txt")
				output_format = "txt"

			if values_file is not None:
				values = []
				with open(values_file, 'r') as values_handle:
					for value in values_handle:
						values.append(value.strip().lower())
				log.info(f"Loaded {len(values)} from values file {values_file}")
			else:
				values = [value.lower() for value in values]  # convert to lowercase

			log.info(f"Filtering field: {field}")
			if len(values) <= 20:
				log.info(f"On values: {','.join(values)}")
			else:
				log.info(f"On values:")
				for value in values:
					log.info(value)
			log.info(f"Exact match {('on' if exact_match else 'off')}. Single field {single_field}.")
			log.info(f"From date {from_date.strftime('%Y-%m-%d')} to date {to_date.strftime('%Y-%m-%d')}")
			log.info(f"Output format set to {output_format}")

			input_files = []
			if os.path.isdir(input_file):
				if not os.path.exists(output_file):
					os.makedirs(output_file)
				for file in os.listdir(input_file):
					if not os.path.isdir(file) and file.endswith(".zst"):
						input_name = os.path.splitext(os.path.splitext(os.path.basename(file))[0])[0]
						input_files.append((os.path.join(input_file, file), os.path.join(output_file, input_name)))
			else:
				input_files.append((input_file, output_file))
			log.info(f"Processing {len(input_files)} files")
			for file_in, file_out in input_files:
				process_file(file_in, file_out, output_format, field, values, from_date, to_date, single_field, exact_match)

# group all the data from different subreddits into one dataframe

import pandas as pd
covid_1 = pd.read_csv('covid_submissions_1.csv')
health = pd.read_csv('health_submissions_1.csv')
india = pd.read_csv('india_submissions_1.csv')
medicine = pd.read_csv('medicine_submissions_1.csv')
health_2 = pd.read_csv('publichealth_submissions_1.csv')
news = pd.read_csv('worldnews_submissions_1.csv')
covid_2 = pd.read_csv('coronavirus_submissions_1.csv')

# rename columns of dump file scrapped data

covid_1_rename = covid_1.rename(columns = {'1': 'upvotes', '2020-03-28': 'date', 'Coronavirus की पहली तस्वीर सामने आई,India के पहले patient से लिया गया था नमूना': 'title', 'u/snehaagg': 'poster', 'https://www.reddit.com/r/COVID/comments/fqmqsp/coronavirus_क_पहल_तसवर_समन_आईindia_क_पहल_patient/': 'url', 'https://www.youtube.com/watch?v=PgfkyAGvOb0': 'other links'})
health_rename = health.rename(columns = {'1': 'upvotes', '2015-01-01': 'date', 'IVF Hospitals In Kerala': 'title', 'u/armcivf': 'poster', 'https://www.reddit.com/r/Health/comments/2qzlob/ivf_hospitals_in_kerala/': 'url', 'http://armcivf.net/blog/causes-infertility-india-among-males/': 'other links'})
india_rename = india.rename(columns = {'1': 'upvotes', '2015-01-01': 'date', 'A Sampling of Indian English Accents': 'title', 'u/[deleted]': 'poster', 'https://www.reddit.com/r/india/comments/2qz0n9/a_sampling_of_indian_english_accents/': 'url', 'https://www.youtube.com/watch?v=v9arM_agKFA': 'other links'})
medicine_rename = medicine.rename(columns = {'11': 'upvotes', '2015-01-14': 'date', 'Does anyone have any experience practicing/working with the Indian Health Service?': 'title', 'u/sojo92': 'poster', 'https://www.reddit.com/r/medicine/comments/2sepkd/does_anyone_have_any_experience_practicingworking/': 'url', 'Looking to get some insight on what your experience was like. Was it rewarding? Did you find the financial incentives (i.e. loan repayment) compelling based on your duties? Would you do it again?': 'other links'})
health_2_rename = health_2.rename(columns = {'19': 'upvotes', '2015-02-20': 'date', 'Drug-resistant malaria is on the verge of entering India (X-post from /r/globalhealth)': 'title', 'u/genericaccount1234': 'poster', 'https://www.reddit.com/r/publichealth/comments/2wjm83/drugresistant_malaria_is_on_the_verge_of_entering/': 'url', 'http://www.bbc.com/news/health-31533559': 'other links'})
news_rename = news.rename(columns = {'1': 'upvotes', '2015-01-01': 'date', "Russia's Strategic Shift To East Continues: Now India": 'title', 'u/AriRusila': 'poster', 'https://www.reddit.com/r/worldnews/comments/2qyydq/russias_strategic_shift_to_east_continues_now/': 'url', 'https://arirusila.wordpress.com/2014/12/17/russias-strategic-shift-to-east-continues-now-india/': 'other links'})
covid_2_rename = covid_2.rename(columns = {'5': 'upvotes', '2020-01-25': 'date', "I'm content that there\'s nothing for Indiana ...yet...": 'title', 'u/BigWhails': 'poster', 'https://www.reddit.com/r/Coronavirus/comments/etmmna/im_content_that_theres_nothing_for_indiana_yet/': 'url', "To be honest, I'm just happy I didn't hear about my state yet...I'm I just by myself here? \n\nI realized how paranoid I'm sounding, but cant blame me, I have family I care about.": 'other links'})

# group datasets from different subreddits into a single dataframe

data2 = covid_1_rename
data2 = data2.append(news_rename, ignore_index = True)
data2 = data2.append(covid_2_rename, ignore_data = covid_1)
data2 = data2.append(health_rename, ignore_index = True)
data2 = data2.append(india_rename, ignore_index = True)
data2 = data2.append(medicine_rename, ignore_index = True)
data2 = data2.append(health_2_rename, ignore_index = Trueindex = True)
data2.to_csv('dataset_india.csv', index = False, encoding = True)
