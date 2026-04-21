import TodayDailyView from './TodayDailyView'

export default function OrchestrationSystemInspector() {
  return (
    <div className="system-inspector">
      <div className="inspector-header card">
        <div className="card-header">
          <h2>Household State Manager</h2>
        </div>
      </div>

      <TodayDailyView />
    </div>
  )
}
