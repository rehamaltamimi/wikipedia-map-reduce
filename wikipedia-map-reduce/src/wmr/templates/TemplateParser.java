/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package wmr.templates;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author shilad
 */
public class TemplateParser {

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
            while (bracketNesting > 0 && j < text.length()) {
                if (text.charAt(j) == '{') {
                    bracketNesting++;
                } else if (text.charAt(j) == '}') {
                    bracketNesting--;
                }
                j++;
            }
            String template = text.substring(i + 2, j - 2);
            templates.add(processTemplate(template, offset+i+2, offset+j-2));
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
                int i = curlyAwareIndexOf(text, '=');
                int j = curlyAwareIndexOf(text, '|');
                String key = null, val = null;

                // positional (unnamed) parameter
                if (i < 0 || (j > 0 && i > j)) {
                    key = "positional" + map.size();
                    val = text.substring(0, j > 0 ? j : text.length());
                } else {
                    key = text.substring(0, i).trim();
                    val = text.substring(i+1, j >0 ? j : text.length());
                }
                text = (j < 0) ? "" : text.substring(j+1);
                if (val != null) {
                    val = val.trim();
                }
                if (val.isEmpty()) {
                    val = null;
                }
                map.put(key, val);
            }
        }
        return new Template(start, end, map);
    }

    private static int curlyAwareIndexOf(String text, char query) {
        int curlyNesting = 0;
        int i = 0;
        for (;; i++) {
            if (i == text.length() || curlyNesting < 0) {
                return -1;
            }
            char c = text.charAt(i);
            if (curlyNesting == 0 && c == query) {
                return i;
            }
            if (c == '{') {
                curlyNesting++;
            }
            if (c == '}') {
                curlyNesting--;
            }
        }
    }
}
