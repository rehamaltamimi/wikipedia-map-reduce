/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser;

import java.util.LinkedHashMap;
import java.util.LinkedList;
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

    /**
     * Find all templates in a page.
     */
    public static List<Template> getOneOrMoreTemplates(String text) {
        return getOneOrMoreTemplates(text, 0);
    }
    public static List<Template> getOneOrMoreTemplates(String text, int offset) {
        List<Template> templates = new LinkedList<Template>();
        int i = 0;
        while (true) {
            long index = text.substring(i).indexOf("{{");
            if (index < 0) {
                break;
            }
            i += index;
            int bracketNesting = 2;
            int j = i + 2;
            while (bracketNesting > 0 & j < text.length()) {
                if (text.charAt(j) == '{') {
                    bracketNesting++;
                } else if (text.charAt(j) == '}') {
                    bracketNesting--;
                }
                j++;
            }
            String template = text.substring(i + 2, j - 2);
            templates.add(Template.processTemplate(template, offset+i+2, offset+j-2));
            i = j;
        }
        return templates;
    }
    
    /**
     * 
     * @param templateSubstring template text (not including before and after brackets)
     * @param start start index
     * @param end end index
     * @return 
     */
    public static Template processTemplate (String templateSubstring, int start, int end) {
        LinkedHashMap<String,String> map = new LinkedHashMap<String,String>();
        String[] mapText = templateSubstring.split("\\|", 2);
        map.put("templateName", mapText[0].trim());

        if (mapText.length > 1) {
            String text = mapText[1];
            while (!text.isEmpty()) {
                String[] keyVal = text.split("=", 2);
                String key = keyVal[0];
                String val = null;
                text = keyVal.length == 1 ? "" : keyVal[1];
                int curlyNesting = 0;
                int i = 0;
                for (;; i++) {
                    if (i == text.length()) {
                        val = text;
                        break;
                    }
                    char c = text.charAt(i);
                    if (curlyNesting == 0 && c == '|') {
                        val = text.substring(0, i);
                        i++;
                        break;
                    }
                    if (c == '{') {
                        curlyNesting++;
                    }
                    if (c == '}') {
                        curlyNesting--;
                    }
                }
                text = text.substring(i);
                if (val != null) {
                    val = val.trim();
                }
                if (val.isEmpty()) {
                    val = null;
                }
                map.put(key.trim(), val);
            }
        }
        return new Template(start, end, map);
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
            return getOneOrMoreTemplates(params.get(param), start);
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
