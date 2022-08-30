
from flask import Flask, jsonify
import sqlalchemy
import numpy as np
import pandas as pd
import datetime as dt

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy.sql.functions import session_user
from sqlalchemy.sql.selectable import subquery

engine = create_engine("sqlite:///Resources/hawaii.sqlite")
Base = automap_base()
Base.prepare(engine, reflect = True)

Measurement = Base.classes.measurement
Station = Base.classes.station

app = Flask(__name__)


@app.route('/')
def welcome():
    return (
        f"<h1> welcome to jae's climate app</h1><br/>"
        f"<h2> this is the solution for challenge #2 on the sqlalchemy-challenge</h2><br/>"
        f"<br/>"
        f"<h3> available routes</h3> <br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/2014-05-01 - enter a date between <strong> 2010-01-01 and 2017-08-23</strong> in that format<br/>"
        f"/api/v1.0/2014-05-01/2015/04/20 - enter a <strong> start and end date </strong> between <strong> 2010-01-01 and 2017-08-23</strong> in that format<br/>"
    )

@app.route('/api/v1.0/precipitation')
def precipitation():
    session = Session(engine)

    lastdate = session.query(func.max(Measurement.date)).\
        scalar()
    dt_lastdate = dt.datetime.strptime(lastdate, "%Y-%m-%d").date()
    dt_startdate = dt_lastdate - dt.timedelta(days=365)
    startdate = dt_startdate.strftime("%Y-%m-%d")
    results = session.query(Measurement.date, Measurement.prcp).\
    filter(Measurement.date.between(startdate, lastdate)).\
    all()
    
    session.close()

    precip = []
    for date, prcp in results:
        precip_dict = {}
        precip_dict['date'] = date
        precip_dict['prcp'] = prcp
        precip.append(precip_dict)
    return jsonify(precip)


@app.route('/api/v1.0/stations')
def stations():

    session = Session(engine)

    results = session.query(Station.name).all()

    session.close()

    all_stations = list(np.ravel(results))
    return jsonify(all_stations)


@app.route('/api/v1.0/tobs')
def tobs():
    session = Session(engine)

    top_station = session.query(Measurement.station).\
        group_by(Measurement.station).\
        order_by(func.count(Measurement.station).desc()).\
        subquery()
    
    lastdate = session.query(func.max(Measurement.date)).\
        scalar()
    dt_lastdate = dt.datetime.strptime(lastdate,"%Y-%m-%d").date()
    dt_startdate = dt_lastdate - dt.timedelta(days = 365)
    startdate = dt_startdate.strftime("%Y-%m-%d")

    results = session.query(Measurement.date, Measurement.tobs).\
    filter(Measurement.date.between(startdate, lastdate)).\
    filter(Measurement.station.in_(top_station)).\
    all()

    session.close()

    topStation= []
    for date, tobs in results:
        tobs_dict = {}
        tobs_dict['date'] = date
        tobs_dict['tobs'] = tobs
        topStation.append(tobs_dict)
    return jsonify(topStation)


@app.route("/api/v1.0/<start>")
@app.route("/api/v1.0/<start>/<end>")
def rangestart(start, end=None):
    session = Session(engine)
    if end == None:
        enddate = session.query(func.max(Measurement.date)).\
            scalar()
        startdate = str(start)
        results = session.query(func.min(Measurement.tobs).label('min temp'),
        func.avg(Measurement.tobs).label('avg_temp'),
        func.max(Measurement.tobs).label('max temp))')).\
        filter(Measurement.date.between(startdate, enddate)).\
        first()
 
    else:
        enddate = str(end)
        startdate = str(start)
        results = session.query(func.min(Measurement.tobs).label('min temp'),
        func.avg(Measurement.tobs).label('avg_temp'),
        func.max(Measurement.tobs).label('max temp))')).\
        filter(Measurement.date.between(startdate, enddate)).\
        first()
    session.close()
    datapoints = list(np.ravel(results))
    return jsonify(datapoints)


if __name__ == "__main__":
    app.run(debug=False)

