"""Create a Flask app to serve the services as REST APIs."""

from flask import Flask, request, jsonify

from services.feed_preprocessing.handler import main as feed_preprocessing_service # noqa
from services.recommendation.handler import main as recommendation_service
from services.feed_postprocessing.handler import main as feed_postprocessing_service # noqa
from services.manage_bluesky_feeds.handler import main as manage_bluesky_feeds_service # noqa

app = Flask(__name__)


@app.route('/preprocess', methods=['POST'])
def preprocess():
    data = request.json
    result = feed_preprocessing_service(data)
    return jsonify(result)


@app.route('/recommend', methods=['POST'])
def recommend():
    data = request.json
    result = recommendation_service(data)
    return jsonify(result)


@app.route('/postprocess', methods=['POST'])
def postprocess():
    data = request.json
    result = feed_postprocessing_service(data)
    return jsonify(result)


@app.route('/manage_feeds', methods=['POST'])
def manage_feeds():
    data = request.json
    result = manage_bluesky_feeds_service(data)
    return jsonify(result)


if __name__ == '__main__':
    app.run(debug=True)
