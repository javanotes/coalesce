package io.reactiveminds.datagrid.mapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

class JSUtil {
	
	public static final String HASH = "#";
	private static final ScriptEngine engine = new ScriptEngineManager().getEngineByName("JavaScript");
	/**
	 * Evaluate expressions on field targets
	 * @param expr
	 * @param val
	 * @return
	 * @throws ScriptException
	 */
	static Object evalExpr(String expr, Object val) throws ScriptException {
		String ev = expr.contains(HASH) ? expr.replaceAll(HASH, val.toString()) : expr;
		System.err.println("eval: "+ev);
		return engine.eval(ev);
	}
	public static final String DOLLAR = "$";
	public static final String OPEN = "{";
	public static final String CLOSE = "}";
	/**
	 * Evaluate a derived expression.
	 * @param params
	 * @param expr
	 * @return
	 * @throws ScriptException
	 */
	static Object evalDerivedExpr(Map<String, Object> params, String expr) throws ScriptException {
		String exprStr = expr;
		for (Entry<String, Object> e : params.entrySet()) {
			exprStr = exprStr.replaceFirst(Pattern.quote(DOLLAR+OPEN+e.getKey()+CLOSE), e.getValue().toString());
		}
		System.err.println("derive: "+exprStr);
		return engine.eval(exprStr);
	}
	private static Map<String, CompiledScript> compiledScripts = new ConcurrentHashMap<>();
	/**
	 * Return the name for the compiled script
	 * @param script
	 * @return
	 * @throws FileNotFoundException
	 * @throws ScriptException
	 */
	static String compile(File script) throws FileNotFoundException, ScriptException {
		if(engine instanceof Compilable) {
			CompiledScript compiled = ((Compilable) engine).compile(new FileReader(script));
			String name = script.getName();
			name = name.substring(0, name.lastIndexOf(".")+1);
			compiledScripts.put(name, compiled);
			return name;
		}
		return NOT_SUPPORTED;
	}
	/**
	 * Execute script identified by given name.
	 * @param scriptName
	 * @param args
	 * @return
	 * @throws ScriptException
	 */
	static Object execute(String scriptName, Map<String, Object> args) throws ScriptException {
		if(compiledScripts.containsKey(scriptName)) {
			Bindings b = engine.createBindings();
			args.forEach((k,v) -> b.put(k, v));
			return compiledScripts.get(scriptName).eval(b);
		}
		return NOT_FOUND;
	}
	public static final String NOT_FOUND = "ERR_SCRIPT_NOT_FOUND";
	public static final String NOT_SUPPORTED = "ERR_SCRIPT_COMPILE_NOT_SUPPORTED";
	
	private static void extractFromJson(String jsonObject) throws ScriptException, NoSuchMethodException {
		String jscript = "function fromJson(in) {var obj=JSON.parse(in);return obj.name;}";
		engine.eval(jscript);
		Invocable invoke = (Invocable) engine;
		Object out = invoke.invokeFunction("fromJson", jsonObject);
		System.out.println(out);
	}
	
	public static void main(String[] args) throws Exception {
		String json = "{name: \"John\", age: 31, city: \"New York\"};";
		extractFromJson(json);
	}
}
