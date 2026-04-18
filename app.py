from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse

from brief_endpoint import run_brief_pipeline
from brief_invariants_v1 import project_brief_to_v1
from brief_renderer_v1 import render_brief_v1
from planning_session import add_session_manual_input, refresh_planning_session

app = FastAPI()
manual_items: list[dict[str, str]] = []


@app.get("/brief")
def get_brief(render_human: bool = False):
    brief = run_brief_pipeline(manual_items=manual_items)

    if render_human:
        brief_v1 = project_brief_to_v1(brief)
        rendered = render_brief_v1(brief_v1)

        return {
            "brief": brief,
            "rendered": rendered,
        }

    return brief


@app.post("/add")
async def add_item(request: Request):
    data = await request.json()
    manual_items.append(data)
    return {"status": "ok", "count": len(manual_items)}


@app.post("/session/add")
async def session_add_item(request: Request):
    data = await request.json()
    if not isinstance(data, dict):
        data = {}

    household_id = str(data.get("household_id", "hh-001"))
    item = {
        "title": data.get("title", ""),
        "type": data.get("type", "task"),
        "time": data.get("time"),
    }

    session = add_session_manual_input(household_id, item)
    return {
        "status": "ok",
        "household_id": session.household_id,
        "count": len(session.manual_inputs),
    }


@app.get("/session/refresh")
def session_refresh(household_id: str = "hh-001"):
    return refresh_planning_session(household_id)


@app.get("/legacy-brief", response_class=HTMLResponse)
def home():
    brief = run_brief_pipeline(manual_items=manual_items)
    brief_v1 = project_brief_to_v1(brief)
    rendered = render_brief_v1(brief_v1)

    html = f"""
    <html>
    <head>
        <title>Daily Brief</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                padding: 40px;
                background: #111;
                color: #eee;
            }}
            pre {{
                background: #1e1e1e;
                padding: 20px;
                border-radius: 8px;
                white-space: pre-wrap;
            }}
            h1 {{
                margin-bottom: 20px;
            }}
            form {{
                display: flex;
                gap: 10px;
                margin-bottom: 20px;
                flex-wrap: wrap;
            }}
            input, select, button {{
                padding: 8px 10px;
                border-radius: 6px;
                border: 1px solid #444;
                background: #181818;
                color: #eee;
            }}
            button {{
                cursor: pointer;
            }}
        </style>
    </head>
    <body>
        <h1>Daily Plan</h1>
        <form method="post" action="/add" onsubmit="submitForm(event)">
            <input type="text" id="title" placeholder="Task or event" required />
            <select id="type">
                <option value="task">Task</option>
                <option value="event">Event</option>
            </select>
            <input type="datetime-local" id="time" />
            <button type="submit">Add</button>
        </form>
        <pre>{rendered}</pre>
        <script>
        async function submitForm(e) {{
            e.preventDefault();

            const title = document.getElementById("title").value;
            const type = document.getElementById("type").value;
            const time = document.getElementById("time").value;

            await fetch("/add", {{
                method: "POST",
                headers: {{"Content-Type": "application/json"}},
                body: JSON.stringify({{ title, type, time }})
            }});

            location.reload();
        }}
        </script>
    </body>
    </html>
    """
    return html
