# -*- coding: utf-8 -*-
# @Time    : 2019/3/28 18:34
# @Author  : DrMa
import psycopg2
from tqdm import tqdm,trange
from someTool import *

#拿到被告人姓名
def get_bgrxx_one(bgrxx):
    jobs = "无职业|无固定职业"
    for gz in dic_gz.keys():
        jobs = jobs + "|" + gz
    # 户籍地、出生地规则
    hj_rules = r'生于\w+?国\w+|' \
               r'生于\w+?省\w+|' \
               r'生于\w+?市\w+|' \
               r'生于\w+?区\w+|' \
               r'生于\w+?县\w+|' \
               r'生于\w+?镇\w+|' \
               r'生于\w+?乡\w+|' \
               r'\w+?国\w{0,30}人|' \
               r'\w+?省\w*?人|' \
               r'\w+?市\w*?人|' \
               r'\w+?区\w*?人|' \
               r'\w+?县\w*?人|' \
               r'\w+?镇\w*?人|' \
               r'\w+?乡\w*?人|' \
               r'生于\w+?县|' \
               r'生于\w+?市'
    name = del_opt(getone(bgrxx, r"被告人（反诉自诉人）\w+?(。|，)|"
                                 r"原审被告人（上诉人）\w+?(。|，)|"
                                 r"被告人\S\w+?(。|，)|"
                                 r"被告\w+?(。|，)|"
                                 r"被告人（自报）\w+?\S|"
                                 r"上诉人.+?(。|，)|"
                                 r"被告人\S\w+（\w+）?(。|，)|"
                                 r"被告人（暨附带民事诉讼被告人）\w+?(。|，)|"
                                 r"被告单位\w+?(。|，)"),
                   ["被告人", "上诉人", "：", "，", "被告", "（自报）", ";", ":", "）", "（", "原审", "。", "暨附带民事诉讼", "单位", "反诉", "自诉人",
                    "申诉人"])
    if name == "":
        name = del_opt(getone(bgrxx, "被告人（自报）\w+?；|"
                                     "上诉人\w+?(。|，)|"
                                     "被告人：\w+?（\w+?：\w+?）|"
                                     "被告人（自报姓名）\w+?(。|，)|"
                                     "被告人（反诉原告人）\w+?(。|，)|"
                                     "被告\w+?（\w+?）|"
                                     "被告人：\w＊"),
                       ["原审被告人（上诉人）", "。", "，", "上诉人", "（", "）", "自报姓名", "被告人", "：", "别名", "反诉", "原告人", "又名", "自报", "；",
                        "被告", "申诉人"])

    if name == "":
        name = del_opt(getone(bgrxx, r'((|原审)上诉人（原审被告人）|被告人)\w+(|＊＊|＊|××|×)\w{0,6}'),
                       ["（", "）", "上诉人", "原审", "被告人"])
    if len(name) > 4:
        name = name[:3]

    # 性别
    if bgrxx.count("男"):
        xb_bg="1"
    elif bgrxx.count("女"):
        xb_bg="2"
    else:
        xb_bg="0"  # 默认值0
    # 出生日期
    csrq = del_opt(getone(bgrxx, r'\d{2,4}年\w{0,10}(出生|生)|生于\d{2,4}年\w{0,10}'),
                   ["生于", "出生", "生"])
    try:
        birth_date = changeStrtoTime(csrq)
    except:
        birth_date = ""
    if birth_date == "":
        birth_date = "1900-01-01"

    # 民族
    def get_mz(texts, rules):
        retes = getall(texts, rules)
        ret = ""
        for rete in retes:
            r = rete.group()
            if r.count("广西") < 1 and r.count("宁夏") < 1 and r.count("自治") < 1 and r.count("区") < 1 \
                    and r.count("市") < 1 and r.count("县") < 1 and r.count("镇") < 1 and r.count("乡") < 1 \
                    and r.count("州") < 1 and r.count("村") < 1:
                ret = r
                break
        return ret

    mz_bg=get_mz(bgrxx, r'\w+?族')
    # 教育程度
    jycd_bg=getone(bgrxx, r'\w+文化|文化程度\w+|研究生|博士|硕士|大学本科|大学|本科|大本|大学专科'
                          r'|大专|专科|中专|中技|高中|初中|小学|文盲|半文盲|职高|无文化')\
        .replace('无文化', '文盲').replace("大本", "本科").replace("大学", "本科")\
        .replace("大学本科", "本科").replace("大学专科", "大专").replace("专科", "大专")
    # 工作
    gz_bg=getone(bgrxx, jobs).replace("无职业", "无业").replace("无固定职业", "无业")
    # 户籍地
    locofbirth = get_real_hj_text(bgrxx, hj_rules)
    if locofbirth != "":
        hj_bg=locofbirth
    else:
        hj_bg= del_opt(getone(bgrxx, r'户籍(|地|所在地)(|\S)\w+'), ["户籍地", "户籍", "所在地", "：", "，", ":"])

    # 居住地,为啥每次都是居住地
    jzd = del_opt(getone(bgrxx, r'住址：\w+|住址\w+|住\w+'), ['住址：', '住址', "住在", "住"])

    # 逮捕机构
    xianan_text = getone(bgrxx, '\w*因本案.*|\w*因涉嫌.*|\w*现因.*|\w*现又涉嫌.*')
    if xianan_text!='':
        bgrxx=xianan_text
    bgrxx=bgrxx.replace(csrq,'')#把出生日期删掉，后边的都是与逮捕相关的日期
    dbjg = del_opt(getone(bgrxx, r'(被|由|到)\w+(|决定)(刑事拘留|取保候审|投案|逮捕|行政拘留|执行逮捕|监视|强制隔离|羁押)'),
                   ["被", "刑事拘留", "由", "到", "决定", "取保候审", "投案", "逮捕", "行政拘留", "执行逮捕", "监视", "强制隔离", "羁押", '依法', '执行',
                    '处以', '采取'])

    # 逮捕日期
    # dbrq_txt = getone(bgrxx.replace("（", "").replace("）", "").replace('、', ""),
    #                   r'\d{2,4}年\d{1,2}月\d{1,2}(|日)(，|\w*?)(被|由|到|经)\w*?(|决定)(刑事拘留|取保候审|投案|逮捕|行政拘留|执行逮捕|监视|强制隔离|羁押|抓获|查获|依法传唤|通知到案|解回)|'
    #                   r'\d{2,4}年\d{1,2}月\d{1,2}(|日)被.*?(刑事拘留|拘留|取保候审|逮捕|行政拘留|监视|强制隔离|羁押|抓获)|'
    #                   r'\d{2,4}年\d{1,2}月\d{1,2}(|日)，(|被告人\w{0,3})因涉嫌\w+?被\w+?(刑事拘留|取保候审|投案|逮捕|行政拘留|执行逮捕|监视|强制隔离|羁押|抓获|查获)|'
    #                   r'\d{2,4}年\d{1,2}月\d{1,2}(|日)(刑事拘留|取保候审|逮捕|行政拘留|监视|强制隔离|羁押|抓获|投案)|'
    #                   r'\d{2,4}年\d{1,2}月\d{1,2}(|日)\w+?(刑事拘留|取保候审|逮捕|行政拘留|监视|强制隔离|羁押|抓获)|'
    #                   r'\d{2,4}年\d{1,2}月\d{1,2}(|日)\w+?暂缓投劳')#只要是当前被控制住了
    qk_texts=get_qkxx_sentence_for_sprq(bgrxx)
    if len(qk_texts)>0:
        for i in qk_texts:
            bgrxx=bgrxx.replace(i,'')
    dbrq_Chinese = getone(bgrxx, r'\d{2,4}年\d{1,2}月\d{1,2}(|日)|'
                                 r'同年\d{1,2}月\d{1,2}(|日)')
    # if dbrq_Chinese.count('同年'):


    try:
        dbrq = changeStrtoTime(dbrq_Chinese)
    except:
        dbrq = "1900-01-01"
    if dbrq == "":
        dbrq = "1900-01-01"
    return name,xb_bg,birth_date,mz_bg,jycd_bg,gz_bg,hj_bg,jzd,dbjg,dbrq
def get_qkxx_sentence_for_sprq(inf_txt_bg):
    qk_sentence=[]
    temp_csrq = getone(inf_txt_bg, r'\d{2,4}年\w{0,10}(出生|生)|生于\d{2,4}年\w{0,10}')\
        .replace("生于", "").replace("出生","").replace("生", "")
    bgrxx = inf_txt_bg.replace(temp_csrq, "")
    xianan_text = getone(bgrxx , '\w*因本案.*|\w*因涉嫌.*|\w*现因.*|\w*现又涉嫌.*')
    bgrxx =bgrxx.replace(xianan_text, '').strip('。').strip('；').strip(';')
    find_fenhao = getone(bgrxx, r'.+?\d{4}年')
    no_fenhao=find_fenhao.replace('；','').replace(';','')#找到第一个分号并去掉，影响后边的split
    bgrxx=bgrxx.replace(find_fenhao,no_fenhao)
    #我们分为两种情况，一种是单条前科，另一种是多条前科，因为多条里可能有都是，的情况。
    if bgrxx.count('因犯')==1 and bgrxx.count('因')==bgrxx.count('因犯') or bgrxx.count('曾犯')==1 or bgrxx.count('因')==1:
        if bgrxx.count("有期徒刑") or bgrxx.count("拘役") or bgrxx.count("管制")or bgrxx.count("行政拘留"):
            qk_sentence.append(bgrxx)
    else:#多条
        if bgrxx.count('；')or bgrxx.count(';'):#有分号，正常用分号分割，然后append
            if bgrxx.count('；'):
                inf_bg_splits =bgrxx.split("；")
            else:
                inf_bg_splits =bgrxx.split(";")
            for inf_bg_split in inf_bg_splits:#行政拘留我们不算
                if inf_bg_split.count("有期徒刑") or inf_bg_split.count("拘役") or inf_bg_split.count("管制")or bgrxx.count("行政拘留"):
                    qk_sentence.append(inf_bg_split)
        else:#没有分号
            find_stop=getone(bgrxx,r'.+?\d{4}年')
            if find_stop.count('。'):#可以用句号
                bgrxx_sent=getone(bgrxx,r'.+?。')
                bgrxx=bgrxx.replace(bgrxx_sent,'')#去掉第一句被告人信息
                if bgrxx.count('。'):#再看有没有句号,有的话直接用句号分
                    inf_bg_splits = bgrxx.split("。")
                    for inf_bg_split in inf_bg_splits:
                        if inf_bg_split.count("有期徒刑") or inf_bg_split.count("拘役") or inf_bg_split.count("管制")or bgrxx.count("行政拘留"):
                            qk_sentence.append(inf_bg_split)
                else:#没有句号，全是逗号，我们用xxx年xx月xx日因来划分
                    qk_sentence=get_all_comma_qk_sent(bgrxx)
            else:#如果第一句都没有句号
                if bgrxx.count('。'):#第一句没有句号，但是后边的有，我们直接split句号
                    inf_bg_splits = bgrxx.split("。")
                    for inf_bg_split in inf_bg_splits:
                        if inf_bg_split.count("有期徒刑") or inf_bg_split.count("拘役") or inf_bg_split.count("管制")or bgrxx.count("行政拘留"):
                            qk_sentence.append(inf_bg_split)
                else:#没有句号，全是逗号
                    qk_sentence= get_all_comma_qk_sent(bgrxx)
    return qk_sentence

conn = psycopg2.connect(database='justice', user='beaver', password='123456', host='58.56.137.206', port='5432')  # 127.0.0.1
cur = conn.cursor()
dic_gz, dic_jycd, dic_mz, dic_spcx, dic_xb, dic_xflb, dic_zm, dic_sheng, dic_shi, dic_xian, dic_wslx = getdics(cur)

cur.execute("select shoubu from bgrspb_xx_test3 where qksl=0 and bgrsl=1 limit 1000")#69w

zws=cur.fetchall()

for i in zws:
    bgrxx=i[0].replace('\n','').replace('\t','')
    name, xb, csrq, mz, jycd, gz, hj, jzd, dbjg, dbrq = get_bgrxx_one(bgrxx)

    if dbrq!='1900-01-01':
        print(bgrxx)
        print(dbrq)



