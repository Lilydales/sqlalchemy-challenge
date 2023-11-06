import datetime as dt
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func,and_
from pathlib import Path

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
#Declare dependancies
database_path= Path("../Resources/hawaii.sqlite")
#Create engine to sqlite
engine=create_engine(f"sqlite:///{database_path}")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save reference to the table
# Assign the station class to a variable called `station`
Station=Base.classes.station
# Assign the measurement class to a variable called `measurement`
Measurement=Base.classes.measurement

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Supporting Functions
#################################################
def date_converter(start):
    converted_date = dt.datetime.strptime(start[-4:] + "-" + start[2:4] + "-" + start[0:2], '%Y-%m-%d').date()
    return converted_date

#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    return (
        f"Welcome to the Climate App!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start&gt<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;<br/>"
        "-------------------------------------------<br/>"
        f"# Note: the date should follow the format ddmmyyyy"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return the precipitation data as json"""
    # Create a session
    session = Session(engine)
    # Request data from the server
    # Find the most recent date in the data set.
    most_recent_record = session.query(Measurement).order_by(Measurement.date.desc()).first()
    # Convert most recent record.date to date time
    most_recent_record_converted = dt.datetime.strptime(most_recent_record.date, '%Y-%m-%d')
    # Calculate the date 12 months ago from the most recent date in the dataset
    twelve_months_ago = most_recent_record_converted - dt.timedelta(days=365)
    # Perform a query to retrieve the data and precipitation scores
    precipitation_result = session.query(Measurement.date, Measurement.prcp,Measurement.station).filter(
        Measurement.date >= twelve_months_ago).all()
    session.close()
    # Create a dictionary from the row data and append to a list
    precipitation_df = []
    for date, prcp,station in precipitation_result:
        precipitation_dic = {}
        precipitation_dic['station_id']=station
        precipitation_dic['date'] = date
        precipitation_dic['prcp'] = prcp
        precipitation_df.append(precipitation_dic)

    return jsonify(precipitation_df)


@app.route("/api/v1.0/stations")
def stations():
    """Return the station data as json"""
    # Create a session
    session = Session(engine)
    # Request data from the server
    stations_query= session.query(Station.station,Station.name).order_by(Station.station.desc()).all()
    session.close()
    # Create a dictionary from the row data and append to a list
    stations_df=[]
    for station,name in stations_query:
        station_dic={}
        station_dic['station']=station
        station_dic['name']=name
        stations_df.append(station_dic)

    return jsonify(stations_df)


@app.route("/api/v1.0/tobs")
def tobs():
    """Return the tobs data as json"""
    # Create a session
    session = Session(engine)
    # Request data from the server
    # Get the most active station
    most_active_station = session.query(Measurement.station, func.min(Measurement.tobs), \
                                        func.max(Measurement.tobs), func.avg(Measurement.tobs)). \
        group_by(Measurement.station). \
        order_by(func.count(Measurement.station).desc()).first()
    # Get the most active station in the recent date
    most_recent_date_most_active_station = session.query(Measurement.date).\
        filter(Measurement.station == most_active_station[0]). \
        order_by(Measurement.date.desc()).first()[0]
    # Convert most recent record.date to date time
    most_recent_date_most_active_station_converted = dt.datetime.strptime(most_recent_date_most_active_station,
                                                                          '%Y-%m-%d')
    # Calculate the date 12 months ago from the most recent date in the dataset
    twelve_months_ago = most_recent_date_most_active_station_converted - dt.timedelta(days=365)
    # Query the last 12 months of temperature observation data for this station
    temperate_most_active_station_last12months = session.query(Measurement.date,Measurement.tobs,Measurement.station). \
        filter(and_(Measurement.date >= twelve_months_ago,Measurement.station == most_active_station[0])).all()
    session.close()
    # Create a dictionary from the row data and append to a list
    tobs_df = []
    for date, temp,station in temperate_most_active_station_last12months:
        tobs_dic = {}
        tobs_dic['station_id']=station
        tobs_dic['date'] = date
        tobs_dic['tobs'] = temp
        tobs_df.append(tobs_dic)

    return jsonify({'station_id':most_active_station[0],
                    'results':tobs_df})

@app.route("/api/v1.0/<start>")
def temperature_info_start(start):
    # Valid the input
    if len(start)==8 and start.isnumeric():
        #Convert the input to date
        converted_date=date_converter(start)

        # Create a session
        session = Session(engine)
        # Request data from the server
        tobs_query = session.query(Measurement.date,\
                                   func.min(Measurement.tobs),func.max(Measurement.tobs),func.avg(Measurement.tobs)).\
            filter(Measurement.date>=converted_date).all()
        session.close()

        if tobs_query[0][0]!=None:
            results_dic={}
            date,min_temp,max_temp,avg=tobs_query[0]
            # Create an empty list for results
            results={'from_date':str(converted_date),
                     'min':min_temp,
                     'max':max_temp,
                     'avg':round(avg,1)
            }

            return jsonify({"results": results}), 200

    return jsonify({"error": f"Data for the date {start} not found."}), 404


@app.route("/api/v1.0/<start>/<end>")
def temperature_info_start_end(start,end):
    # Valid the input
    if len(start+end)==16 and start.isnumeric() and end.isnumeric():
        #Convert the input to date
        converted_start_date=date_converter(start)
        converted_end_date=date_converter(end)

        # Create a session
        session = Session(engine)
        # Request data from the server
        tobs_query = session.query(Measurement.date,\
                                   func.min(Measurement.tobs),func.max(Measurement.tobs),func.avg(Measurement.tobs)).\
            filter(and_(Measurement.date>=converted_start_date,Measurement.date<=converted_end_date)).all()
        session.close()

        if tobs_query[0][0]!=None:
            results_dic={}
            date,min_temp,max_temp,avg=tobs_query[0]
            # Create an empty list for results
            results={'date':str(converted_start_date) + " / " + str(converted_end_date),
                     'min':min_temp,
                     'max':max_temp,
                     'avg':round(avg,1)
            }

            return jsonify({"results": results}), 200

    return jsonify({"error": f"Data for the date {start}/{end} not found."}), 404


if __name__ == "__main__":
    app.run(debug=True)
