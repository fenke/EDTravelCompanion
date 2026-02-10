function CodeBlock(el)
  local filtered = {}
  for line in el.text:gmatch("[^\r\n]+") do
    if not line:match("^%%%%") then
      table.insert(filtered, line)
    end
  end
  el.text = table.concat(filtered, "\n")
  return el
end
