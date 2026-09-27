use devo_protocol::{
    ModelResponse, ResponseContent, ResponseMetadata, StopReason, StreamEvent, Usage,
};

pub fn tool_call_events(id: &str, name: &str, input: serde_json::Value) -> Vec<StreamEvent> {
    vec![
        StreamEvent::ToolCallStart {
            index: 0,
            id: id.to_string(),
            name: name.to_string(),
            input: input.clone(),
        },
        StreamEvent::MessageDone {
            response: ModelResponse {
                id: format!("response-{id}"),
                content: vec![ResponseContent::ToolUse {
                    id: id.to_string(),
                    name: name.to_string(),
                    input,
                }],
                stop_reason: Some(StopReason::ToolUse),
                usage: Usage::default(),
                metadata: ResponseMetadata::default(),
            },
        },
    ]
}

pub fn text_events(text: &str) -> Vec<StreamEvent> {
    vec![
        StreamEvent::TextDelta {
            index: 0,
            text: text.to_string(),
        },
        StreamEvent::MessageDone {
            response: ModelResponse {
                id: "response-final".to_string(),
                content: vec![ResponseContent::Text(text.to_string())],
                stop_reason: Some(StopReason::EndTurn),
                usage: Usage::default(),
                metadata: ResponseMetadata::default(),
            },
        },
    ]
}
