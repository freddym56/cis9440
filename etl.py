import csv
import cx_Oracle
import kaggle
import os

class OlympicGames:

    def __init__(self):

        cx_Oracle.init_oracle_client(lib_dir=os.environ.get('oracle_client'))
        self.connection = cx_Oracle.connect(user=os.environ.get('oracle_user'), password=os.environ.get('oracle_password'), dsn="cis9440_high")
        self.cursor = self.connection.cursor()


    def cleanup(self):
        self.connection.close()


    def create_tables(self):
        '''This method wil create all of the tables.
           It will drop the tables if they already exist.
        '''
        dim_city = '''CREATE TABLE dim_city (
                            city_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY kEY, 
                            city_name VARCHAR2(128),
                            CONSTRAINT city_unique UNIQUE (city_name)
                      )
                   '''

        dim_season = '''CREATE TABLE dim_season (
                            season_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY kEY, 
                            season_name VARCHAR2(128),
                            CONSTRAINT season_unique UNIQUE (season_name)
                      )
                   '''

        dim_sport = '''CREATE TABLE dim_sport (
                            sport_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY kEY, 
                            sport_name VARCHAR2(128),
                            CONSTRAINT sport_unique UNIQUE (sport_name)
                      )
                   '''
    
        dim_medal = '''CREATE TABLE dim_medal (
                            medal_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY kEY, 
                            medal_color VARCHAR2(128),
                            CONSTRAINT medal_unique UNIQUE (medal_color)
                      )
                   '''

        dim_country = '''CREATE TABLE dim_country (
                            country_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY kEY, 
                            country_code CHAR(3),
                            country_name VARCHAR2(128),
                            country_gdp_per_cap NUMBER,
                            country_population NUMBER,
                            CONSTRAINT country_unique UNIQUE (country_code, country_name)
                      )
                   '''

        dim_year = '''CREATE TABLE dim_year (
                            year_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY kEY, 
                            year NUMBER,
                            CONSTRAINT year_unique UNIQUE (year)
                      )
                   '''

        dim_event = '''CREATE TABLE dim_event (
                            event_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY kEY, 
                            event_name VARCHAR2(128),
                            CONSTRAINT event_unique UNIQUE (event_name)
                      )
                   '''

        dim_athlete = '''CREATE TABLE dim_athlete (
                            athlete_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY kEY, 
                            athlete_name VARCHAR2(128),
                            CONSTRAINT athlete_unique UNIQUE (athlete_name)
                      )
                   '''

        dim_gender = '''CREATE TABLE dim_gender (
                            gender_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY kEY, 
                            gender_name VARCHAR2(128),
                            CONSTRAINT gender_unique UNIQUE (gender_name)
                      )
                   '''
        fact_olympic_events = '''CREATE TABLE fact_olympic_events (
                                    id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY kEY,
                                    city_id NUMBER,
                                    season_id NUMBER,
                                    sport_id NUMBER,
                                    medal_id NUMBER,
                                    country_id NUMBER,
                                    year_id NUMBER,
                                    event_id NUMBER,
                                    athlete_id NUMBER,
                                    gender_id NUMBER,
                                    FOREIGN KEY (city_id)
                                        REFERENCES dim_city(city_id)
                                        ON DELETE CASCADE,
                                    FOREIGN KEY (season_id)
                                        REFERENCES dim_season(season_id)
                                        ON DELETE CASCADE,
                                    FOREIGN KEY (sport_id)
                                        REFERENCES dim_sport(sport_id)
                                        ON DELETE CASCADE,
                                    FOREIGN KEY (medal_id)
                                        REFERENCES dim_medal(medal_id)
                                        ON DELETE CASCADE,
                                    FOREIGN KEY (country_id)
                                        REFERENCES dim_country(country_id)
                                        ON DELETE CASCADE,
                                    FOREIGN KEY (year_id)
                                        REFERENCES dim_year(year_id)
                                        ON DELETE CASCADE,
                                    FOREIGN KEY (event_id)
                                        REFERENCES dim_event(event_id)
                                        ON DELETE CASCADE,
                                    FOREIGN KEY (athlete_id)
                                        REFERENCES dim_athlete(athlete_id)
                                        ON DELETE CASCADE,
                                    FOREIGN KEY (gender_id)
                                        REFERENCES dim_gender(gender_id)
                                        ON DELETE CASCADE
                              )
                              '''
        self._drop_if_exist('fact_olympic_events')
        self._drop_if_exist('dim_city')
        self.cursor.execute(dim_city)

        self._drop_if_exist('dim_season')
        self.cursor.execute(dim_season)

        self._drop_if_exist('dim_sport')
        self.cursor.execute(dim_sport)

        self._drop_if_exist('dim_medal')
        self.cursor.execute(dim_medal)

        self._drop_if_exist('dim_country')
        self.cursor.execute(dim_country)

        self._drop_if_exist('dim_year')
        self.cursor.execute(dim_year)

        self._drop_if_exist('dim_event')
        self.cursor.execute(dim_event)

        self._drop_if_exist('dim_athlete')
        self.cursor.execute(dim_athlete)

        self._drop_if_exist('dim_gender')
        self.cursor.execute(dim_gender)

        self.cursor.execute(fact_olympic_events)

        self.connection.commit()


    def extract(self):
        # this method downloads csv files from kaggle
        kaggle.api.dataset_download_files(dataset='the-guardian/olympic-games', unzip=True)
        files = kaggle.api.dataset_list_files(dataset='the-guardian/olympic-games').files
        print(f'Downloaded {len(files)} files.')
        for file in files:
            print(file)


    def transform_and_load(self):
        # this method will transform data from csv
        # into dimensions and fact and load into the database.
        unique_values = self._get_unique_values()
        self._load_rows_for_table('dim_city', 'city_name', unique_values['City'])
        self._load_rows_for_table('dim_sport', 'sport_name', unique_values['Sport'])
        self._load_rows_for_table('dim_medal', 'medal_color', unique_values['Medal'])
        self._load_rows_for_table('dim_year', 'year', unique_values['Year'])
        self._load_rows_for_table('dim_event', 'event_name', unique_values['Event'])
        self._load_rows_for_table('dim_athlete', 'athlete_name', unique_values['Athlete'])
        self._load_rows_for_table('dim_gender', 'gender_name', unique_values['Gender'])
        self._load_seasons()
        self._load_country()
        self._load_fact_table()


    def _drop_if_exist(self, table_name):
        try:
            self.cursor.execute(f'DROP TABLE {table_name}')
        except:
            pass
    
    def _get_unique_values(self):
        unique_values = {}
        for file in ('winter.csv', 'summer.csv'):
            with open(file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    for column, value in row.items():
                        if column in unique_values:
                            unique_values[column].add(value)
                        else:
                            unique_values[column] = {value}
        return unique_values


    def _load_rows_for_table(self, table_name, column_name, values):
        self.cursor.executemany(f'INSERT INTO {table_name} ({column_name}) VALUES (:{column_name})', [[value] for value in values])
        self.connection.commit()


    def _load_seasons(self):
        seasons = ('winter', 'summer')

        self.cursor.executemany(f'INSERT INTO dim_season (season_name) VALUES (:season_name)', [[season] for season in seasons])
        self.connection.commit()


    def _load_country(self):
        rows = []
        with open('dictionary.csv', 'r') as f:
            fields = f.readline().replace(' ', '').strip().split(',')
            reader = csv.DictReader(f, fieldnames=fields)
            for row in reader:
                row['Country'] = row['Country'].replace('*', '')
                rows.append(row)
        self.cursor.executemany(f'''INSERT INTO dim_country (country_code, country_name, country_gdp_per_cap, country_population) VALUES (:Code, :Country, :GDPperCapita, :Population)''', rows)
        self.connection.commit()


    def _get_dict_from_query(self, query):
        self.cursor.execute(query)
        return {row[1]: row[0] for row in self.cursor.fetchall()}


    def _load_fact_table(self):
        # unfortunately need to get ids from data we already loaded
        cities = self._get_dict_from_query('select * from dim_city')
        seasons = self._get_dict_from_query('select * from dim_season')
        sports = self._get_dict_from_query('select * from dim_sport')
        medals = self._get_dict_from_query('select * from dim_medal')
        countries = self._get_dict_from_query('select * from dim_country')
        years = self._get_dict_from_query('select * from dim_year')
        events = self._get_dict_from_query('select * from dim_event')
        athletes = self._get_dict_from_query('select * from dim_athlete')
        genders = self._get_dict_from_query('select * from dim_gender')
        data_rows = []
        for file in ('winter.csv', 'summer.csv'):
            with open(file, 'r') as f:
                reader = csv.DictReader(f)
                season_id = seasons[file.replace('.csv', '')]
                for row in reader:
                    data = {
                        'city_id':  cities[row['City']],
                        'season_id': season_id,
                        'sport_id': sports[row['Sport']],
                        'medal_id': medals[row['Medal']],
                        'country_id': countries.get(row['Country']),
                        'year_id': years[int(row['Year'])],
                        'event_id': events[row['Event']],
                        'athlete_id': athletes[row['Athlete']],
                        'gender_id': genders[row['Gender']]
                    }
                    data_rows.append(data)
        self.cursor.executemany('''INSERT INTO fact_olympic_events (city_id, season_id, sport_id, medal_id, country_id, year_id, event_id, athlete_id, gender_id) VALUES (:city_id, :season_id, :sport_id, :medal_id, :country_id, :year_id, :event_id, :athlete_id, :gender_id)''', data_rows)
        self.connection.commit()

if __name__ == '__main__':
    etl = OlympicGames()
    etl.create_tables()
    etl.extract()
    etl.transform_and_load()
    etl._load_fact_table()
    etl.cleanup()
    