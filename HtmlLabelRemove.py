#-*-coding:utf8-*-
"""
author:wangyue
date:20161220
删除标签
"""

import re
import json

def rm_html_label(str):
    if not str:
        return str
    
    # html label
    re_script=re.compile('<\s*script[^>]*>',re.I) #script
    re_end_script=re.compile('<\s*/\s*script\s*>',re.I) #/script
    re_style=re.compile('<\s*style[^>]*>',re.I) #style
    re_end_style=re.compile('<\s*/\s*style\s*>',re.I) #style
    re_div=re.compile('<\s*div[^>]*>',re.I) #p
    re_end_div=re.compile('<\s*/\s*div\s*>',re.I) #p
    re_p=re.compile('<\s*p[^>]*>',re.I) #p
    re_end_p=re.compile('<\s*/\s*p\s*>',re.I) #p
    re_span=re.compile('<\s*span[^>]*>',re.I) #p
    re_end_span=re.compile('<\s*/\s*span\s*>',re.I) #p
    re_u=re.compile('<\s*u[^>]*>',re.I) #p
    re_end_u=re.compile('<\s*/\s*u\s*>',re.I) #p
    re_td=re.compile('<\s*td[^>]*>',re.I) #p
    re_end_td=re.compile('<\s*/\s*td\s*>',re.I) #p
    re_tr=re.compile('<\s*tr[^>]*>',re.I) #p
    re_end_tr=re.compile('<\s*/\s*tr\s*>',re.I) #p
    re_table=re.compile('<\s*table[^>]*>',re.I) #p
    re_end_table=re.compile('<\s*/\s*table\s*>',re.I) #p
    re_tbody=re.compile('<\s*tbody[^>]*>',re.I) #p
    re_end_tbody=re.compile('<\s*/\s*tbody\s*>',re.I) #p
    re_strong=re.compile('<\s*strong[^>]*>',re.I) #p
    re_end_strong=re.compile('<\s*/\s*strong\s*>',re.I) #p
    re_v=re.compile('<\s*v:[^>]*>',re.I) #p
    re_end_v=re.compile('<\s*/\s*v:[^>]*\s*>',re.I) #p
    re_o=re.compile('<\s*o:[^>]*>',re.I) #p
    re_end_o=re.compile('<\s*/\s*o:[^>]*\s*>',re.I) #p
    re_w=re.compile('<\s*w:[^>]*>',re.I) #p
    re_end_w=re.compile('<\s*/\s*w:[^>]*\s*>',re.I) #p
    re_m=re.compile('<\s*m:[^>]*>',re.I) #p
    re_end_m=re.compile('<\s*/\s*m:[^>]*\s*>',re.I) #p
    re_firefox_if=re.compile('<\s*!--\[if.*<!\[endif\]-->',re.I) #p
    re_firefox_if2=re.compile('<!--\[ifgtemso9\]>.*<!\[endif\]-->',re.I) #p
    re_firefox_if3=re.compile('<!--\[ifgtemso10\]>.*<!\[endif\]-->',re.I) #p
    re_firefox_endif=re.compile('<!--!\[endif\]---->',re.I) #p
    re_img=re.compile('<\s*img[^>]*/>',re.I) #p
    re_br =re.compile('<br>',re.I)
       
    
    str = re_br.sub('', str)
    str = re_img.sub('', str)
    str = re_script.sub('', str)
    str = re_end_script.sub('', str)
    str = re_style.sub('', str)
    str = re_end_style.sub('', str)
    str = re_div.sub('', str)
    str = re_end_div.sub('', str)
    str = re_p.sub('', str)
    str = re_end_p.sub('', str)
    str = re_span.sub('', str)
    str = re_end_span.sub('', str)
    str = re_u.sub('', str)
    str = re_end_u.sub('', str)
    str = re_strong.sub('', str)
    str = re_end_strong.sub('', str)
    str = re_td.sub('', str)
    str = re_end_td.sub('', str)
    str = re_tr.sub('', str)
    str = re_end_tr.sub('', str)
    str = re_table.sub('', str)
    str = re_end_table.sub('', str)
    str = re_tbody.sub('', str)
    str = re_end_tbody.sub('', str)
    str = re_v.sub('', str)
    str = re_end_v.sub('', str)
    str = re_o.sub('', str)
    str = re_end_o.sub('', str)
    str = re_w.sub('', str)
    str = re_end_w.sub('', str)
    str = re_m.sub('', str)
    str = re_end_m.sub('', str)
    str = re_firefox_if.sub('', str)
    str = re_firefox_if2.sub('', str)
    str = re_firefox_if3.sub('', str)
    str = re_firefox_endif.sub('', str)

    # text 
    #str = str.replace(' ', '')
    str = str.replace('\n', ' ').replace('＂',' ').replace('"',' ').replace('\'',' ').replace("]",' ')
    str = str.replace('\t', ' ').replace('“',' ').replace("[", ' ').replace('‘',' ').replace('’',' ')
    str = str.replace('.', ' ').replace('”',' ').replace("…", ' ')
    str = str.replace('．', ' ').replace("·",' ').replace("(",' ').replace(")", ' ')
    str = str.replace('，', ' ')
    str = str.replace('、', ' ')
    str = str.replace('；', ' ')
    str = str.replace('？', ' ')
    str = str.replace('。', ' ')
    str = str.replace('；', ' ')
    str = str.replace('！', ' ')
    str = str.replace(': ', ' ')
    str = str.replace(',', ' ')
    str = str.replace(':', ' ')
    str = str.replace('】', ' ')
    str = str.replace('【',' ')
    str = str.replace('\\,', ' ')
    str = str.replace('\\', ' ')
    str = str.replace('_', ' ')
    str = str.replace('：', ' ')
    str = str.replace('（', ' ')
    str = str.replace('）', ' ')
    str = str.replace('\\!', ' ')
    str = str.replace('\\[', ' ')
    str = str.replace('\\]', ' ')
    str = str.replace('$$', ' ')
    str = str.replace('\\x', ' ')
    str = str.replace('\\y', ' ')
    str = str.replace('\\z', ' ')
    str = str.replace('\\(', ' ')
    str = str.replace('\\)', ' ')
    str = str.replace('\\{', ' ')
    str = str.replace('\\}', ' ')
    str = str.replace(',}', ' ')
    str = str.replace(',)', ' ')
    
    #english letter
    el_regx = re.compile('[A-Za-z]')
    str = el_regx.sub('',str)

    return " ".join(str.split())
    




if __name__ == '__main__':
    
    s = "$$a={{2}^{0.3}}$$，$$b=\\ln \\frac{1}{2}$$，$$c=\\sin 1$$，则$$a$$，$$b$$，$$c$$之间的大小关系是<u></u>．"
    rm_html_label(s)
