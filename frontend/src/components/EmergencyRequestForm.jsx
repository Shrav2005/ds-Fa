export default function EmergencyRequestForm({ onSubmit, loading, disabled, patientName }) {
  const handleSubmit = (event) => {
    event.preventDefault();

    const form = event.currentTarget;
    const generatedId = `P-${String(Date.now()).slice(-6)}`;
    const bedType = form.bed_type.value;
    const priority = form.priority.value;

    onSubmit({
      patient_id: generatedId,
      patient_name: patientName,
      bed_type: bedType,
      priority,
    });

    form.reset();
  };

  return (
    <section className="panel">
      <div className="section-header">
        <h2>Emergency Bed Request</h2>
      </div>

      <form className="request-form" onSubmit={handleSubmit}>
        <div className="field-row">
          <label>
            Bed Type
            <select name="bed_type" defaultValue="ICU">
              <option value="ICU">ICU</option>
              <option value="Emergency">Emergency</option>
              <option value="General">General</option>
            </select>
          </label>

          <label>
            Priority
            <select name="priority" defaultValue="Critical">
              <option value="Critical">Critical</option>
              <option value="High">High</option>
              <option value="Normal">Normal</option>
            </select>
          </label>
        </div>

        <button type="submit" disabled={loading || disabled}>
          {loading ? 'Searching...' : disabled ? 'Complete the current handoff first' : 'Find Available Hospital'}
        </button>
      </form>
    </section>
  );
}
