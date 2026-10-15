export default function AllocationResult({
  result,
  onClear,
  consultationComplete,
  confirming,
  onConfirmReservation,
  onCancelConsultation,
}) {
  return (
    <section className="panel result-panel">
      <div className="section-header">
        <h2>Bed request update</h2>
        {onClear && result?.status !== 'CONSULTATION_REQUIRED' && (
          <button className="link-button" onClick={onClear}>Clear</button>
        )}
      </div>

      {!result ? (
        <p className="muted-text">No bed request has been submitted.</p>
      ) : (
        <>
          <div className={`alert ${result.success ? 'success' : 'error'}`}>
            {result.message}
          </div>

          {result.success && result.selected_hospital && (
            <div className="result-details">
              <p><strong>Patient ID:</strong> {result.patient_id}</p>
              <p><strong>Bed type:</strong> {result.bed_type}</p>
              <p><strong>Receiving hospital:</strong> {result.selected_hospital.hospital_name}</p>
              <p><strong>Hospital ID:</strong> {result.selected_hospital.hospital_id}</p>
              {result.selected_hospital.available_beds !== undefined && (
                <p><strong>Beds available before consultation:</strong> {result.selected_hospital.available_beds}</p>
              )}
              {result.selected_hospital.remaining_beds !== undefined && (
                <p><strong>Beds remaining:</strong> {result.selected_hospital.remaining_beds}</p>
              )}
            </div>
          )}

          {result.status === 'CONSULTATION_REQUIRED' && (
            <div className="consultation-next-step">
              <button
                type="button"
                disabled={!consultationComplete || confirming}
                onClick={onConfirmReservation}
              >
                {confirming ? 'Confirming...' : 'Confirm'}
              </button>
              <button
                type="button"
                className="secondary-button"
                disabled={confirming}
                onClick={onCancelConsultation}
              >
                Cancel request
              </button>
            </div>
          )}

          {result.success && !result.selected_hospital && (
            <div className="result-details">
              <p><strong>Status:</strong> Placement released.</p>
            </div>
          )}

        </>
      )}

    </section>
  );
}
