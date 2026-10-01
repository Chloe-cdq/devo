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

/// Old wrapper journals lack the execution marker. Admit only statically
/// inspectable local calls and output of their results.
fn legacy_wrapper_is_local(root: tree_sitter::Node<'_>, code: &[u8]) -> bool {
    if root.has_error() {
        return false;
    }
    let mut results = Vec::new();
    let mut saw_local_call = false;
    let mut cursor = root.walk();
    for statement in root.named_children(&mut cursor) {
        match statement.kind() {
            "comment" => {}
            "expression_statement" => {
                let Some(expression) = statement.named_child(0) else {
                    return false;
                };
                if local_tool_call(expression, code) {
                    saw_local_call = true;
                } else if !result_output(expression, code, &results) {
                    return false;
                }
            }
            "lexical_declaration" => {
                if statement
                    .child(0)
                    .is_none_or(|keyword| keyword.kind() != "const")
                    || statement.named_child_count() != 1
                {
                    return false;
                }
                let Some(declaration) = statement.named_child(0) else {
                    return false;
                };
                let Some(name) = declaration.child_by_field_name("name") else {
                    return false;
                };
                if name.kind() != "identifier" {
                    return false;
                }
                let Ok(name) = name.utf8_text(code) else {
                    return false;
                };
                if matches!(name, "tools" | "text") || results.contains(&name) {
                    return false;
                }
                let Some(value) = declaration.child_by_field_name("value") else {
                    return false;
                };
                if !local_tool_call(value, code) {
                    return false;
                }
                results.push(name);
                saw_local_call = true;
            }
            _ => return false,
        }
    }
    saw_local_call
}

fn result_output(expression: tree_sitter::Node<'_>, code: &[u8], results: &[&str]) -> bool {
    if expression.kind() != "call_expression"
        || expression
            .child_by_field_name("function")
            .and_then(|callee| callee.utf8_text(code).ok())
            != Some("text")
    {
        return false;
    }
    let Some(arguments) = expression.child_by_field_name("arguments") else {
        return false;
    };
    let mut cursor = arguments.walk();
    let mut children = arguments.named_children(&mut cursor);
    let Some(value) = children.next() else {
        return false;
    };
    if children.next().is_some() {
        return false;
    }
    result_value(value, code, results)
}

fn result_value(value: tree_sitter::Node<'_>, code: &[u8], results: &[&str]) -> bool {
    match value.kind() {
        "identifier" => value
            .utf8_text(code)
            .is_ok_and(|name| results.contains(&name)),
        "member_expression" => {
            value
                .child_by_field_name("property")
                .is_some_and(|property| property.kind() == "property_identifier")
                && value
                    .child_by_field_name("object")
                    .is_some_and(|object| result_value(object, code, results))
        }
        "parenthesized_expression" => {
            value.named_child_count() == 1
                && value
                    .named_child(0)
                    .is_some_and(|inner| result_value(inner, code, results))
        }
        "binary_expression" => {
            value
                .child_by_field_name("operator")
                .is_some_and(|operator| operator.kind() == "??")
                && value
                    .child_by_field_name("left")
                    .is_some_and(|left| result_value(left, code, results))
                && value.child_by_field_name("right").is_some_and(|right| {
                    result_value(right, code, results) || static_wrapper_argument(right)
                })
        }
        _ => false,
    }
}

fn local_tool_call(expression: tree_sitter::Node<'_>, code: &[u8]) -> bool {
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
