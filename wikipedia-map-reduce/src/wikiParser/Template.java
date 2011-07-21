/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package wikiParser;

import java.util.LinkedHashMap;

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
     * 
     * @param templateSubstring template text (not including before and after brackets)
     * @param start start index
     * @param end end index
     * @return 
     */
    public static Template processTemplate (String templateSubstring, int start, int end) {
        String[] mapText = templateSubstring.split("\\|");
        LinkedHashMap<String,String> map = new LinkedHashMap<String,String>();
        map.put("templateName", mapText[0].trim());
        for (int i = 1; i < mapText.length; i++) { //skip first because that is template name
            String[] keyVal = mapText[i].split("=", 2);
            if (keyVal.length == 2) {
                String key = keyVal[0].trim();
                String val = keyVal[1].trim();
                if (!val.isEmpty()) {
                    map.put(key, val);
                }
            }
        }
        return new Template(start, end, map);
    }
    
    public int getStart() {
        return start;
    }
    
    public int getEnd() {
        return end;
    }
    
    public String getParam(String param) {
        return params.get(param);
    }
    
    public LinkedHashMap<String,String> getAllParams() {
            return params;
    }
    
    public void putParam(String param, String value) {
        params.put(param,value);
    }
    
    public String toString() {
        StringBuffer sb = new StringBuffer("<template from " + start + " to " + end + ":");
        for (String key : params.keySet()) {
            sb.append(" ('" + key + "', '" + params.get(key) + "')");
        }
        sb.append(">");
        return sb.toString();
    }
}
