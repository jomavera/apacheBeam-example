import argparse
import logging
import typing
import datetime, os
import apache_beam as beam
from apache_beam.dataframe.convert import to_dataframe, to_pcollection
import math
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import RuntimeValueProvider

#---#---#---# CUSTOM OPTIONS #---#---#---#

class MyOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_value_provider_argument('--input_dir', type=str)
    parser.add_value_provider_argument('--output_dir', type=str)

#---#---#---# SCHEMAS #---#---#---#

class CityData(typing.NamedTuple):
    avg_median_age : float
    avg_male_population : float
    avg_female_population : float
    avg_total_population : float
    avg_number_veterans : float
    avg_foreign_born : float
    avg_average_household_size : float
    i94addr : str

class ImmigrationData(typing.NamedTuple):
    arrdate: int 
    depdate: int
    i94mon: int
    i94visa: int
    i94port: str
    i94addr: str 
    biryear: int
    gender: str

class AirportData(typing.NamedTuple):
    municipality: str
    i94port: str

class TemperatureData(typing.NamedTuple):
    dt: datetime.datetime
    AverageTemperature: float
    AverageTemperatureUncertainty: float
    City: str
    Country: str
    Latitude: str
    Longitude: str


#---#---#---#---#---#---#---#---#---#

class AverageDictFn(beam.CombineFn):
  def create_accumulator(self):
    sum = {}
    count = {}
    accumulator = sum, count
    return accumulator

  def add_input(self, accumulator, input):
    sum, count = accumulator
    for key in list(input.keys()):
        if input[key] != None:
            if key in sum:
                sum[key] += input[key]
                count[key] += 1
            else:
                sum[key] = 0
                count[key] = 0
                sum[key] += input[key]
                count[key] += 1
    return sum, count

  def merge_accumulators(self, accumulators):
    sums, counts = zip(*accumulators)

    sum_all = {}
    count_all = {}
    for sum in sums:
        for key in list(sum.keys()):
            if key in sum_all:
                sum_all[key] += sum[key]
                count_all[key] += 1
            else:
                sum_all[key] = 0
                count_all[key] = 0
                sum_all[key] += sum[key]
                count_all[key] += 1
    return sum_all, count_all

  def extract_output(self, accumulator):
    sum, count = accumulator

    avg = {}

    for key in list(sum.keys()):
        
        key_output = 'avg_'+key

        if count[key] == 0:
            avg[key_output] = float('NaN')
        else:
            avg[key_output] = sum[key]/count[key]

    return avg

#---#---#---# TRANSFORMATIONS #---#---#---#

class SplitAirportData(beam.DoFn):
    def process(self, element):

        ident, tipo, name, elevation_ft, continent, iso_country, \
            iso_region, municipality, gps_code, iata_code, \
                local_code = element.split(',')
        
        return [{
            'i94port': str(iata_code),
            'municipality': str(municipality)
        }]


class SplitCityData(beam.DoFn):
    def process(self, element):

        city, state, median_age, male_population, female_population,\
        total_population, number_veterans, foreign_born,\
        average_household_size, state_code, race, count = element.split(';')
        
        return [(
            state_code,{
                'median_age': float(median_age) if median_age != '' else None,
                'male_population': int(male_population) if male_population != '' else None,
                'female_population': int(female_population) if female_population != '' else None,
                'total_population': int(total_population) if total_population != '' else None,
                'number_veterans': int(number_veterans) if number_veterans != '' else None,
                'foreign_born': int(foreign_born) if foreign_born != '' else None,
                'average_household_size' : float(average_household_size) if average_household_size != '' else None
            }   
        )]

class SplitTempData(beam.DoFn):
    def process(self, element):
        dt, AverageTemperature, AverageTemperatureUncertainty,\
        City, Country, Latitude, Longitude = element.split(',')
        
        return [({
                'dt': datetime.datetime.strptime(dt, '%Y-%m-%d') if dt != '' else None,
                'AverageTemperature': float(AverageTemperature) if AverageTemperature != '' else None,
                'AverageTemperatureUncertainty': float(AverageTemperatureUncertainty) if AverageTemperatureUncertainty != '' else None,
                'City': str(City) if City != '' else None,
                'Country': str(Country) if Country != '' else None,
                'Latitude': str(Latitude) if Latitude != '' else None,
                'Longitude': str(Longitude) if Longitude != '' else None
            }   
        )]
class OrganizeCityData(beam.DoFn):

    def process(self, element):

        key, values = element

        values['i94addr'] = key

        return [values]

def ToRowImmigration(values):

    return beam.Row(
        arrdate = int(values['arrdate']) if values['arrdate'] != None else 0,
        depdate = int(values['depdate']) if values['depdate'] != None else 0,
        i94mon = int(values['i94mon']) if 'i94mon' in values.keys() else int('NaN'),
        i94visa = int(values['i94visa']) if 'i94visa' in values.keys() else int('NaN'),
        i94port = str(values['i94port']) if 'i94port' in values.keys() else str('NaN'),
        i94addr = str(values['i94addr']) if 'i94addr' in values.keys() else str('NaN'),
        biryear = int(values['biryear']) if values['biryear'] != None in values.keys() else 0,
        gender = str(values['gender']) if 'gender' in values.keys() else str('NaN')
    )

def ToRowCity(values):

    return beam.Row(
        avg_median_age = float(values['avg_median_age']) if 'avg_median_age' in values.keys() else float('NaN'),
        avg_male_population = float(values['avg_male_population']) if 'avg_male_population' in values.keys() else float('NaN'),
        avg_female_population = float(values['avg_female_population']) if 'avg_female_population' in values.keys() else float('NaN'),
        avg_total_population = float(values['avg_total_population']) if 'avg_total_population' in values.keys() else float('NaN'),
        avg_number_veterans = float(values['avg_number_veterans']) if 'avg_number_veterans' in values.keys() else float('NaN'),
        avg_foreign_born = float(values['avg_foreign_born']) if 'avg_foreign_born' in values.keys() else float('NaN'),
        avg_average_household_size = float(values['avg_average_household_size']) if 'avg_average_household_size' in values.keys() else float('NaN'),
        i94addr = str(values['i94addr'])
    )

def ToRowAirport(values):

    return beam.Row(
        i94port = str(values['i94port']) if 'i94port' in values.keys() else str('NaN'),
        municipality = str(values['municipality']) if 'municipality' in values.keys() else str('NaN')
    )

class FilterAndToRowTemperature(beam.DoFn):
    def process(self, element):

        if (element['dt'] > datetime.datetime(2011,12,31,23,59,0,0)) and (element['dt'] < datetime.datetime(2013,1,1,0,0,0,0)):
            return [beam.Row(
                dt = element['dt'] if 'dt' in element.keys() else str('NaN'),
                avg_temp = float(element['AverageTemperature']) if 'AverageTemperature' in element.keys() else str('NaN'),
                municipality = str(element['City']) if 'City' in element.keys() else str('NaN'),
                country = str(element['Country']) if 'Country' in element.keys() else str('NaN'),
                month = element['dt'].month if 'dt' in element.keys() else str('NaN')
            )]

def run():
    
    options = MyOptions()

    with beam.Pipeline(options=options) as p:

            immigration_data = (
                p | 'Read Immigration Data' >> beam.io.parquetio.ReadFromParquet(p.options.input_dir.get()+'data.parquet\*').with_output_types(ImmigrationData) |
                'Immigration dictionary collection to row' >> beam.Map(ToRowImmigration) 
            )

            df_immigration = to_dataframe(immigration_data)

            cities_data = (
                p | 'Read city data' >> beam.io.ReadFromText(p.options.input_dir.get()+'us-cities-demographics.csv', skip_header_lines=1) |
                'Parse city data' >> beam.ParDo(SplitCityData()) |
                'Get average demographics per State' >> beam.CombinePerKey(AverageDictFn())  |
                'Key to Column' >> beam.ParDo(OrganizeCityData()).with_output_types(CityData) |
                'City dictionary collection to row' >> beam.Map(ToRowCity)
            )

            df_cities = to_dataframe(cities_data)

            df_immigration = df_immigration.join(df_cities, rsuffix='_city')

            airport_data = (
                p | "Read airport data" >> beam.io.ReadFromText(p.options.input_dir.get()+'airport-codes_csv_2.csv', skip_header_lines=1) |
                "Parse airport data" >> beam.ParDo(SplitAirportData()).with_output_types(AirportData) |
                'Airport dictionary collection to row' >> beam.Map(ToRowAirport)
            )

            df_airport = to_dataframe(airport_data)

            join_data = df_immigration.join(df_airport, rsuffix='_airport')

            join_data = to_pcollection(join_data, include_indexes=False)

            #---#---#---# TODO #---#---#---#
            #-------- Join temperature data to dataset --------

            # temperature_data = (
            #     p | "Read city data" >> beam.io.ReadFromText('GlobalLandTemperaturesByCity.csv', skip_header_lines=1) |
            #     "Parse temperature data" >> beam.ParDo(SplitTempData()).with_output_types(TemperatureData)  |
            #     'Temperature dictionary filtered and to row' >> beam.ParDo(FilterAndToRowTemperature())
            # )
            
            #-------- NotImplementedError: grouby(as_index = False) and drop_duplicates()--------
            # df_immigration_2 = df_immigration.filter(
            #         items=['arrdate','i94mon','municipality','i94port']
            #     ).groupby(
            #         by = ['arrdate','i94mon','municipality','i94port'], as_index = False
            #     )

            # df_temperature = to_dataframe(temperature_data)

            # df_temperature_2 = df_immigration_2.join(df_temperature,
            #                                 (df_immigration_2.municipality == df_temperature.municipality) \
            #                                 & (df_immigration_2.i94mon == df_temperature.month ), 'left')

            output = join_data | beam.io.WriteToText(p.options.output_dir.get())

if __name__ == '__main__':
    run()