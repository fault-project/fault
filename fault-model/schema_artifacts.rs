use fault_model::{JournalEvent, Run, RunProgress, RunResult};
use schemars::schema_for;
use serde_json::Value;
use std::collections::BTreeSet;

pub fn schemas() -> Vec<(&'static str, Value)> {
    vec![
        ("run.schema.json", value(schema_for!(Run))),
        ("run-progress.schema.json", value(schema_for!(RunProgress))),
        ("run-result.schema.json", value(schema_for!(RunResult))),
        ("journal-event.schema.json", value(schema_for!(JournalEvent))),
    ]
}

fn value(schema: impl serde::Serialize) -> Value {
    serde_json::to_value(schema).expect("schema is serializable")
}

pub fn reference_html(schemas: &[(&str, Value)]) -> String {
    let mut sections = String::new();
    let mut rendered_definitions = BTreeSet::new();
    for (file, schema) in schemas {
        render_schema(&mut sections, file, schema, &mut rendered_definitions);
    }

    format!(
        r##"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="description" content="Generated reference for fault run files, events, and results.">
<title>fault reference</title>
<script>(()=>{{let t;try{{t=localStorage.getItem("fault-theme")}}catch{{}}t??=matchMedia("(prefers-color-scheme:light)").matches?"light":"dark";document.documentElement.dataset.theme=t}})();</script>
<style>
:root{{--bg:#151513;--panel:#20201d;--ink:#f5f5ed;--muted:#aaa99f;--line:#393934;--accent:#ffef0c;--code:#0e0e0d;color-scheme:dark}}
:root[data-theme="light"]{{--bg:#fff;--panel:#f5f5f3;--ink:#171714;--muted:#686862;--line:#d9d9d2;--accent:#ffef0c;--code:#20201d;color-scheme:light}}
*{{box-sizing:border-box}} html{{scroll-behavior:smooth}} body{{margin:0;background:var(--bg);color:var(--ink);font:16px/1.6 system-ui,sans-serif}}
main{{max-width:1040px;margin:auto;padding:3rem 1.4rem 6rem}} a{{color:inherit;text-decoration-color:#a99e00;text-underline-offset:.18em}}
h1{{font-size:clamp(2.5rem,7vw,5rem);line-height:1;margin:.3rem 0 1rem}} h2{{font-size:1.8rem;margin-top:4rem;border-top:1px solid var(--line);padding-top:2rem}}
h3{{font-size:1.2rem;margin-top:2.5rem}} .eyebrow,.badge{{font:700 .75rem ui-monospace,monospace;text-transform:uppercase;letter-spacing:.1em}}
.eyebrow{{display:inline-block;background:var(--accent);color:#171714;padding:.25rem .55rem}} .lede{{max-width:720px;color:var(--muted);font-size:1.15rem}}
nav{{display:flex;flex-wrap:wrap;gap:.65rem;margin:2rem 0}} nav a,.badge,.theme-toggle{{border:1px solid var(--line);border-radius:999px;padding:.25rem .65rem;text-decoration:none}}
.theme-toggle{{background:var(--panel);color:var(--ink);font:inherit;cursor:pointer}} .theme-toggle:hover,.theme-toggle:focus-visible{{border-color:var(--accent)}}
table{{width:100%;border-collapse:collapse;margin:1rem 0 2rem}} th,td{{padding:.7rem;text-align:left;vertical-align:top;border-bottom:1px solid var(--line)}} th{{font-size:.78rem;text-transform:uppercase;letter-spacing:.06em;color:var(--muted)}}
code{{font-family:ui-monospace,SFMono-Regular,monospace}} pre{{overflow:auto;background:var(--code);color:#f4f4ec;padding:1.1rem;border-left:5px solid var(--accent);border-radius:.25rem}}
.required{{color:#9a4100;font-weight:700}} .muted{{color:var(--muted)}} details{{background:var(--panel);padding:.8rem 1rem;margin:.7rem 0;border:1px solid var(--line)}} summary{{cursor:pointer;font-weight:700}}
</style></head><body><main>
<span class="eyebrow">Generated reference</span><h1>fault, field by field</h1>
<p class="lede">The exact wire format for run files, live progress, completed results, and journal records. This page is generated from the Rust model beside the JSON Schemas; do not edit it by hand.</p>
<nav><a href="index.html">Guide</a><button class="theme-toggle" id="theme-toggle" type="button">theme</button><a href="#run-schema-json">Run files</a><a href="#fault-compatibility">Fault compatibility</a><a href="#run-progress-schema-json">Progress</a><a href="#run-result-schema-json">Results</a><a href="#journal-event-schema-json">Journal</a></nav>
<h2 id="run-shape">The smallest useful run</h2>
<p>A run declares one or more proxies and an ordered list of phases. A phase without <code>duration</code> remains active until stopped and must be last.</p>
<pre><code>schema_version: 1
name: slow database
proxies:
  - name: database
    protocol: tcp
    listen: 127.0.0.1:15432
    upstream: database.internal:5432
phases:
  - name: degraded
    proxies:
      - proxy: database
        faults:
          - type: latency
            flow: both
            distribution: {{ type: normal, mean_ms: 200, stddev_ms: 20 }}</code></pre>
<h2 id="fault-compatibility">Fault and transport compatibility</h2>
<table><thead><tr><th>Fault</th><th>TCP</th><th>UDP</th><th>Meaning</th></tr></thead><tbody>
<tr><td><code>latency</code></td><td>yes</td><td>yes</td><td>Delay selected traffic using a sampled distribution.</td></tr>
<tr><td><code>jitter</code></td><td>yes</td><td>yes</td><td>Probabilistically delay selected traffic.</td></tr>
<tr><td><code>bandwidth</code></td><td>yes</td><td>no</td><td>Limit bytes per second for each connection stream.</td></tr>
<tr><td><code>blackhole</code></td><td>yes</td><td>yes</td><td>Leave TCP traffic pending or drop UDP datagrams.</td></tr>
<tr><td><code>connection-reset</code></td><td>yes</td><td>no</td><td>Reset a TCP connection when selected traffic is first used.</td></tr>
<tr><td><code>dns</code></td><td>no</td><td>yes</td><td>Alter DNS queries carried by a UDP proxy.</td></tr>
</tbody></table>
{sections}
<p class="muted">Regenerate with <code>cargo run -p fault-model --example generate_schemas</code>.</p>
</main><script>
const toggle=document.querySelector("#theme-toggle");
function showTheme(theme){{document.documentElement.dataset.theme=theme;toggle.textContent="theme: "+theme;toggle.setAttribute("aria-label","Switch to "+(theme==="dark"?"light":"dark")+" theme")}}
showTheme(document.documentElement.dataset.theme);
toggle.addEventListener("click",()=>{{const theme=document.documentElement.dataset.theme==="dark"?"light":"dark";showTheme(theme);try{{localStorage.setItem("fault-theme",theme)}}catch{{}}}});
</script></body></html>
"##
    )
}

fn render_schema(
    output: &mut String,
    file: &str,
    schema: &Value,
    rendered_definitions: &mut BTreeSet<String>,
) {
    let id = file.replace('.', "-");
    let title = schema["title"].as_str().unwrap_or(file);
    output.push_str(&format!("<h2 id=\"{}\">{}</h2>", html(&id), html(title)));
    if let Some(description) = schema["description"].as_str() {
        output.push_str(&format!("<p>{}</p>", html(description)));
    }
    output.push_str(&format!(
        "<p><a href=\"schemas/{}\">Open the JSON Schema</a></p>",
        html(file)
    ));
    render_shape(output, title, schema);
    if let Some(definitions) = schema["$defs"].as_object() {
        for (name, definition) in definitions {
            if !rendered_definitions.insert(name.clone()) {
                continue;
            }
            output.push_str(&format!(
                "<details><summary>{}</summary>",
                html(name)
            ));
            render_shape(output, name, definition);
            output.push_str("</details>");
        }
    }
}

fn render_shape(output: &mut String, name: &str, schema: &Value) {
    if let Some(variants) = schema["oneOf"].as_array() {
        output.push_str("<table><thead><tr><th>Variant</th><th>Fields</th><th>Description</th></tr></thead><tbody>");
        for variant in variants {
            let variant_name = variant["properties"]["type"]["const"]
                .as_str()
                .or_else(|| variant["const"].as_str())
                .or_else(|| variant["enum"][0].as_str())
                .unwrap_or("variant");
            output.push_str(&format!(
                "<tr><td><code>{}</code></td><td>{}</td><td>{}</td></tr>",
                html(variant_name),
                property_summaries(variant),
                html(variant["description"].as_str().unwrap_or(""))
            ));
        }
        output.push_str("</tbody></table>");
        return;
    }
    let Some(properties) = schema["properties"].as_object() else {
        output.push_str(&format!(
            "<p><code>{}</code>: {}</p>",
            html(name),
            describe(schema)
        ));
        return;
    };
    let required = schema["required"].as_array();
    output.push_str("<table><thead><tr><th>Field</th><th>Required</th><th>Type and constraints</th><th>Description</th></tr></thead><tbody>");
    for (field, definition) in properties {
        let is_required = required.is_some_and(|items| {
            items.iter().any(|item| item.as_str() == Some(field))
        });
        output.push_str(&format!("<tr><td><code>{}</code></td><td class=\"{}\">{}</td><td>{}</td><td>{}</td></tr>", html(field), if is_required { "required" } else { "muted" }, if is_required { "yes" } else { "no" }, describe(definition), html(definition["description"].as_str().unwrap_or(""))));
    }
    output.push_str("</tbody></table>");
}

fn property_summaries(schema: &Value) -> String {
    schema["properties"]
        .as_object()
        .map(|properties| {
            properties
                .keys()
                .filter(|key| key.as_str() != "type")
                .map(|key| {
                    format!(
                        "<code>{}</code>: {}",
                        html(key),
                        describe(&properties[key])
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default()
}

fn describe(schema: &Value) -> String {
    if let Some(reference) = schema["$ref"].as_str() {
        return format!(
            "<code>{}</code>",
            html(reference.rsplit('/').next().unwrap_or(reference))
        );
    }
    if let Some(values) = schema["enum"].as_array() {
        return values
            .iter()
            .filter_map(Value::as_str)
            .map(|v| format!("<code>{}</code>", html(v)))
            .collect::<Vec<_>>()
            .join(" | ");
    }
    if let Some(constant) = schema.get("const") {
        return format!(
            "constant <code>{}</code>",
            html(&constant.to_string())
        );
    }
    if let Some(items) = schema.get("items") {
        return format!("array of {}{}", describe(items), constraint(schema));
    }
    if let Some(options) = schema["anyOf"].as_array() {
        if let Some(value) = options
            .iter()
            .find(|option| option["type"].as_str() != Some("null"))
        {
            return format!("{} or null", describe(value));
        }
    }
    if let Some(types) = schema["type"].as_array() {
        let names = types
            .iter()
            .filter_map(Value::as_str)
            .map(html)
            .collect::<Vec<_>>()
            .join(" or ");
        return format!("{}{}", names, constraint(schema));
    }
    let kind = schema["type"].as_str().unwrap_or_else(|| {
        if schema.get("anyOf").is_some() { "optional value" } else { "object" }
    });
    format!("{}{}", html(kind), constraint(schema))
}

fn constraint(schema: &Value) -> String {
    let mut values = Vec::new();
    for (key, label) in [
        ("minimum", "min"),
        ("exclusiveMinimum", ">"),
        ("maximum", "max"),
        ("minLength", "min length"),
        ("minItems", "min items"),
    ] {
        if let Some(value) = schema.get(key) {
            values.push(format!("{} {}", label, html(&value.to_string())));
        }
    }
    if values.is_empty() {
        String::new()
    } else {
        format!(" <span class=\"muted\">({})</span>", values.join(", "))
    }
}

fn html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
