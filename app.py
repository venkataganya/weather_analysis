from flask import Flask, jsonify
from db_write.database_read_write import DbWrite
from extract_data.extract import logging



db = DbWrite(db = 'db/WeatherData.db')
app = Flask(__name__)

@app.route('/api/weather/<lower>/<upper>', methods=['GET'])
def api_get_weather_form_data(lower,upper):
    """
    API endpoint: http://127.0.0.1:5000/api/weather/1/100
    """
    data = db.get_transformed_data(lower,upper)
    return jsonify(data)


@app.route('/api/weather/stats/<lower>/<upper>', methods=['GET'])
def api_get_weather_agg_data(lower,upper):
    """
    API endpoint: http://127.0.0.1:5000/api/weather/stats/1/100
    """
    data = db.get_aggregate_data(lower,upper)
    return jsonify(data)

@app.route('/api/weather/transf/count', methods=['GET'])
def api_get_transf_count():
    """
    API endpoint: http://127.0.0.1:5000/api/weather/transf/count
    """
    data = db.get_count_transf()
    return jsonify(data)

@app.route('/api/weather/agg/count', methods=['GET'])
def api_get_aggr_count():
    """
    API endpoint: http://127.0.0.1:5000/api/weather/agg/count
    """
    data = db.get_count_agg()
    return jsonify(data)


@app.route('/api/weather/filter/<region>/<temperature_recorded_date>', methods=['GET'])
def api_get_weather_form_data_yr_reg(region,temperature_recorded_date):
    """
    API endpoint: http://127.0.0.1:5000/api/weather/filter/USC00336196/1985-04-10
    """
    data = db.get_transformed_data_by_region_date(region,temperature_recorded_date)
    return jsonify(data)    


@app.route('/api/weather/stats/filter/<avg_year>/<region>', methods=['GET'])
def api_get_weather_agg_data_yr_reg(avg_year,region):
    """
    API endpoint: http://127.0.0.1:5000/api/weather/stats/filter/1995/USC00132977
    """
    data = db.get_aggregate_data_by_region_year(avg_year,region)
    return jsonify(data)

if __name__ == "__main__":
    logging.info("Running flask App")
    app.run(debug=True, threaded = True) #run app