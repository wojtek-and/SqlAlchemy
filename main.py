import csv
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

# Tworzenie bazy danych SQLite w pamięci
engine = create_engine('sqlite:///database2.db', echo=True)

Base = declarative_base()


# Definicja tabeli Station
class Station(Base):
    __tablename__ = 'stations'

    station = Column(String, primary_key=True)
    latitude = Column(String, nullable=False)
    longitude = Column(String, nullable=False)
    elevation = Column(String, nullable=False)
    name = Column(String, nullable=False)
    country = Column(String, nullable=False)
    state = Column(String, nullable=False)

    measures = relationship("Measure", back_populates="stat")


# Definicja tabeli Measure
class Measure(Base):
    __tablename__ = 'measure'

    id = Column(Integer, primary_key=True)
    station = Column(String, ForeignKey('stations.station'))
    date = Column(String, nullable=False)
    precip = Column(String, nullable=False)
    tobs = Column(Integer, nullable=False)

    stat = relationship("Station", back_populates="measures")


# Tworzenie tabel w bazie danych
Base.metadata.create_all(engine)

# Tworzenie sesji
Session = sessionmaker(bind=engine)
session = Session()


# Funkcja do wczytywania danych z pliku CSV
def load_csv_data(file_name):
    with open(file_name, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        data = [row for row in reader]
    return data


# Załadowanie danych z plików CSV
stations_data = load_csv_data('clean_stations.csv')
measures_data = load_csv_data('clean_measure.csv')

# Dodanie danych do tabel
for row in stations_data:
    station = Station(station=str(row['station']), latitude=row['latitude'], longitude=row['longitude'],
                      elevation=row['elevation'], name=row['name'], country=row['country'], state=row['state'])
    session.add(station)

for index, row in enumerate(measures_data):
    measure = Measure(id=index, station=str(row['station']), date=row['date'], precip=row['precip'], tobs=row['tobs'])
    session.add(measure)

# Zatwierdzenie zmian
session.commit()
