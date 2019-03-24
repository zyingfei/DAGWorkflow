Workflow Stop On Error Notes:

Workflow activities' Stop on Error is only completely work as expected if they meet following requirement:
1. Context is handled properly in activities.
- Context.Done() is passed into every async calls.
- Return is ErrCanceled on context.Done()
2. Task uses heartbeat to track.
- Task cannot be canceled if it is enabled with heartbeat.
- It will only be canceled when heartbeat calls or timeout.

Same rules applies to other features related to ActivityCancel event.