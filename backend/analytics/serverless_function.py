"""Knative HTTP function for on-demand, de-identified analytics aggregation."""

from flask import Flask, jsonify, request

from analytics.streaming_common import aggregate_records


app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 8 * 1024 * 1024


@app.route("/healthz", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/", methods=["POST"])
def aggregate():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict) or not isinstance(payload.get("records"), list):
        return jsonify({"success": False, "message": "A JSON object with a records array is required."}), 400
    if any(not isinstance(record, dict) for record in payload["records"]):
        return jsonify({"success": False, "message": "Each analytics record must be a JSON object."}), 400
    try:
        summary = aggregate_records(payload["records"])
    except (AttributeError, TypeError, ValueError) as exc:
        return jsonify({"success": False, "message": str(exc)}), 400
    return jsonify({"summary": summary})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
