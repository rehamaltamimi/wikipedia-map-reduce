/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wmr.templates;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author research
 */
public class Template {
    private int start;//index template text starts at
    private int end;//index template ends at
    private LinkedHashMap<String,String> params; //all other information
    
    public Template (int start, int end, LinkedHashMap<String,String> params) {
        this.start = start;
        this.end = end;
        this.params = params;
    }


    public String getName() {
        return params.get("templateName");
    }
    
    public int getStart() {
        return start;
    }
    
    public int getEnd() {
        return end;
    }

    public boolean hasParam(String param) {
        return params.containsKey(param);
    }

    public String getParam(String param) {
        return params.get(param);
    }

    public List<Template> getParamAsTemplate(String param) {
        if (paramContainsTemplate(param)) {
            return TemplateParser.getOneOrMoreTemplates(params.get(param), start);
        } else {
            return null;
        }
    }

    public boolean paramContainsTemplate(String param) {
        String val = params.get(param);
        return val != null && val.startsWith("{{") && val.endsWith("}}");
    }

    public LinkedHashMap<String,String> getAllParams() {
            return params;
    }
    
    public void putParam(String param, String value) {
        params.put(param,value);
    }

    public void convertMapToLowercase() {
        LinkedHashMap<String, String> newParams = new LinkedHashMap<String, String>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            String k = entry.getKey();
            String v = entry.getValue();
            if (!k.equals("templateName")) {
                k = k.toLowerCase();
            }
            v = (v == null) ? null : v.toLowerCase();
            newParams.put(k, v);
        }
        this.params = newParams;
    }
    
    public String toString() {
        StringBuffer sb = new StringBuffer("<template from " + start + " to " + end + ":");
        for (String key : params.keySet()) {
            String val = params.get(key);
            if (val != null && val.length() > 50) {
                val = val.substring(0, 47) + "...";
            }
            sb.append(" ('" + key + "', '" + val + "')");
        }
        sb.append(">");
        return sb.toString();
    }
}
