pub(super) fn external_tool(name: &str, input: Option<&serde_json::Value>) -> bool {
    if external_tool_name(name) {
        return true;
    }
    if name != "functions.exec" {
        return false;
    }
    let Some(code) = input
        .and_then(|value| value.get("code"))
        .and_then(|value| value.as_str())
    else {
        return true;
    };
    let mut parser = tree_sitter::Parser::new();
    if parser
        .set_language(&tree_sitter_javascript::LANGUAGE.into())
        .is_err()
    {
        return true;
    }
    let Some(tree) = parser.parse(code, None) else {
        return true;
    };
    !legacy_wrapper_is_local(tree.root_node(), code.as_bytes())
}

pub(super) fn external_tool_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    let name = name.strip_prefix("tools.").unwrap_or(&name);
    name.starts_with("web__")
        || name.starts_with("mcp__")
        || name.starts_with("mcp.")
        || name.starts_with("browser.")
        || name.starts_with("browser__")
        || matches!(
            name,
            "web.run"
                | "web_search"
                | "websearch"
                | "web-search"
                | "web_fetch"
                | "webfetch"
                | "toolsearch"
                | "tool-search"
                | "tool_search"
                | "tools_search"
                | "search_tools"
                | "loadtool"
                | "functions.tool_search"
        )
}

/// Old wrapper journals lack the execution marker. Admit only a direct local
/// tool call with literal arguments; arbitrary JavaScript can hide tool use.
fn legacy_wrapper_is_local(root: tree_sitter::Node<'_>, code: &[u8]) -> bool {
    if root.has_error() || root.named_child_count() != 1 {
        return false;
    }
    let Some(statement) = root.named_child(0) else {
        return false;
    };
    if statement.kind() != "expression_statement" {
        return false;
    }
    let Some(expression) = statement.named_child(0) else {
        return false;
    };
    let call = if expression.kind() == "await_expression" {
        expression.named_child(0)
    } else {
        Some(expression)
    };
    let Some(call) = call.filter(|call| call.kind() == "call_expression") else {
        return false;
    };
    let Some(callee) = call.child_by_field_name("function") else {
        return false;
    };
    if callee.kind() != "member_expression"
        || callee
            .child_by_field_name("object")
            .and_then(|object| object.utf8_text(code).ok())
            != Some("tools")
    {
        return false;
    }
    let local_tool = callee
        .child_by_field_name("property")
        .and_then(|property| property.utf8_text(code).ok());
    if !matches!(
        local_tool,
        Some("exec_command" | "apply_patch" | "write_stdin" | "view_image")
    ) {
        return false;
    }
    let Some(arguments) = call.child_by_field_name("arguments") else {
        return false;
    };
    let mut cursor = arguments.walk();
    let mut children = arguments.named_children(&mut cursor);
    children.next().is_some_and(static_wrapper_argument) && children.next().is_none()
}

fn static_wrapper_argument(node: tree_sitter::Node<'_>) -> bool {
    match node.kind() {
        "string" | "number" | "true" | "false" | "null" => true,
        "array" => {
            let mut cursor = node.walk();
            node.named_children(&mut cursor)
                .all(static_wrapper_argument)
        }
        "object" => {
            let mut cursor = node.walk();
            node.named_children(&mut cursor).all(|pair| {
                pair.kind() == "pair"
                    && pair.child_by_field_name("key").is_some_and(|key| {
                        matches!(key.kind(), "property_identifier" | "string" | "number")
                    })
                    && pair
                        .child_by_field_name("value")
                        .is_some_and(static_wrapper_argument)
            })
        }
        _ => false,
    }
}
