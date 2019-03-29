import re
import datetime
import uuid

def getone(texts,rules):
    pattern=re.compile(rules,re.DOTALL)
    rete=pattern.search(texts)
    if rete is not None:
        return rete.group()
    else:
        return ""
def getall(texts,rules):
    pattern=re.compile(rules)
    retes=pattern.finditer(texts)
    return retes
#在getall基础上改进，直接返回正则匹配后的list
def get_all_text(texts,rules):
    list=[]
    pattern = re.compile(rules)
    retes = pattern.finditer(texts)
    for rete in retes:
        list.append(rete.group())
    return list

#增加删除操作，让代码简洁
def del_opt(org_txt, word_replace_list):
    for word in word_replace_list:
        org_txt=org_txt.replace(word,'')
    return org_txt

def changeStrtoTime(str_text):
    re = ""
    for s in str_text:
        re = re + SBC2DBC(s)
    str_text = re
    re = ""
    # print(str_text)
    strtoalb = {'0': '0', '1': '1', '2': '2', '3': '3', '4': '4', '5': '5', '6': '6',
                '7': '7', '8': '8', '9': '9', '10': '10', '11': '11', '12': '12', '13': '13', '14': '14',
                '15': '15', '16': '16', '17': '17', '18': '18', '19': '19', '20': '20', '21': '21',
                '22': '22', '23': '23', '24': '24', '25': '25', '26': '26', '27': '27','28': '28', '29': '29',
                '30': '30', '31': '31', '〇': '0', '零': '0','○':'0','1O':'10','一': '1', '元': '1', '二': '2', '两': '2', '三': '3',
                '四': '4', '五': '5', '六': '6','七': '7', '八': '8', '九': '9', '十': '10', '十一': '11', '十二': '12', '十三': '13', '十四': '14',
                '十五': '15', '十六': '16', '十七': '17', '十八': '18', '十九': '19', '二十': '20', '二十一': '21',
                '二十二': '22', '二十三': '23', '二十四': '24', '二十五': '25', '二十六': '26', '二十七': '27',
                '二十八': '28', '二十九': '29', '三十': '30', '三十一': '31'}
    for i in range(1,10):
        temp_key='0' + str(i)
        strtoalb[temp_key]=str(i)
    nian = str_text.split("年")#nian[0]是年份，nian[1]是月份加号
    if len(nian) == 2:
        for s in nian[0]:
            if s in strtoalb:
                re = re + strtoalb[s]
        re = re + "-"
        # print(re)
        yue = nian[1].split("月")
        if len(yue) == 2:
            if yue[0] in strtoalb:
                re = re + strtoalb[yue[0]] + "-"
            # print(re)
            ri = yue[1].split("日")
            if ri[0] in strtoalb:
                re = re + strtoalb[ri[0]]
            # print(re)
    try:
        datetime.datetime.strptime(re, '%Y-%m-%d')
    except:
        re = ""
    return re
def SBC2DBC(char):
    chr_code = ord(char)
    # 处理全角中数字大等于10的情况
    if chr_code in range(9312, 9332):
        return str(chr_code - 9311)
    elif chr_code in range(9332, 9352):
        return str(chr_code - 9331)
    elif chr_code in range(9352, 9372):
        return str(chr_code - 9351)
    elif chr_code in range(8544, 8556):
        return str(chr_code - 8543)

    else:
        if chr_code == 12288: # 全角空格，同0x3000
            chr_code = 32
        if chr_code == 8216 or chr_code == 8217:  # ‘’
            chr_code = 39 # '
        elif chr_code in range(65281, 65374):
            chr_code = chr_code - 65248
        return chr(chr_code)
#字典相关
def todic(list):#给定二维list,构建字典
    dic = {}
    for l in list:
        dic[l[0]] = l[1]#key值对应value
    return dic
def getdics(cur):#工作，教育程度，民族，审判程序，性别，刑法类别，罪名，省市县。{值：代号}
    cur.execute("select value, key from dic where type = 'gz'")
    gz = todic(cur.fetchall())
    cur.execute("select value, key from dic where type = 'jycd'")
    jycd = todic(cur.fetchall())
    cur.execute("select value, key from dic where type = 'mz'")
    mz = todic(cur.fetchall())
    cur.execute("select value, key from dic where type = 'spcx'")
    spcx = todic(cur.fetchall())
    cur.execute("select value, key from dic where type = 'gz'")
    xb = todic(cur.fetchall())
    cur.execute("select value, key from dic where type = 'xflb'")
    xflb = todic(cur.fetchall())
    cur.execute("select value, key from dic where type = 'zm'")
    zm = todic(cur.fetchall())
    cur.execute("select value, key from dic where type = 'sheng'")
    sheng = todic(cur.fetchall())
    cur.execute("select value, key from dic where type = 'shi'")
    shi = todic(cur.fetchall())
    cur.execute("select value, key from dic where type = 'xian'")
    xian = todic(cur.fetchall())
    cur.execute("select value, key from dic where type = 'wslx'")
    wslx=todic(cur.fetchall())
    return gz, jycd, mz, spcx, xb, xflb, zm, sheng, shi, xian,wslx
def getids(str, dic):
    re = "0"
    if str != "":
        if str in dic:#直接判断是否在字典里,dic是[value:代码。。。。]
            re = dic[str]
        else:#如果匹配不上字典，那么用字典匹配字符串
            for s in dic.keys():#其实只留下这个for循环就可以，s.count(str)和上边的判断重复
                if s.count(str) > 0 or str.count(s) > 0:
                    re = dic[s]
                    break
    return re
#四个函数，将中文数字转换成digit
def changeMoneytoInt(str):
    if str.count("全部"):
        return "-1"
    else:
        str = Chin2Num(str)
        if str == "-1":
            re = "-1"
        else:
            num = getone(str, r'\d+')
            re = ""
            for n in num:
                re = re + SBC2DBC(n)
            if re == "":
                re = "0"
        return re
def Chin2Num(Str):
    re = 0
    if Str.count("亿"):
        re = -1
    else:
        w = Str.count("万")
        if w > 0:
            for s in Str.split("万"):
                wan = getone(str(changeChineseNumToArab(s)), r'\d+')
                if wan != '':
                    re = re * 10000 + int(wan)
                else:
                    re = re * 10000
                w -= 1
            while w > 0:
                re = re * 10000
                w -= 1
        else:
            wan = getone(str(changeChineseNumToArab(Str)), r'\d+')
            if wan == '':
                re = 0
            else:
                re = int(wan)
    return repr(re)
def changeChineseNumToArab(oriStr):
    num_str_start_symbol = ['一', '二', '两', '三', '四', '五', '六', '七', '八', '九','十', '壹', '贰', '叁', '肆', '伍', '陆', '柒', '捌', '玖', '拾']
    more_num_str_symbol = ['零', '一', '二', '两', '三', '四', '五', '六', '七', '八', '九', '十', '百', '千', '万', '亿', '壹', '贰', '叁'
        , '肆', '伍', '陆', '柒', '捌', '玖', '拾', '佰', '仟', '萬']

    lenStr = len(oriStr)
    aProStr = ''
    if lenStr == 0:
        return aProStr

    hasNumStart = False
    numberStr = ''
    for idx in range(lenStr):
        if oriStr[idx] in num_str_start_symbol:
            if not hasNumStart:
                hasNumStart = True

            numberStr += oriStr[idx]
        else:
            if hasNumStart:
                if oriStr[idx] in more_num_str_symbol:
                    numberStr += oriStr[idx]
                    continue
                else:
                    numResult = str(chinese2digits(numberStr))
                    numberStr = ''
                    hasNumStart = False
                    aProStr += numResult

            aProStr += oriStr[idx]
            pass

    if len(numberStr) > 0:
        resultNum = chinese2digits(numberStr)
        aProStr += str(resultNum)
    return aProStr
def chinese2digits(uchars_chinese):
    common_used_numerals_tmp = {'零': 0, '一': 1, '二': 2, '两': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9, '十': 10,
                                '百': 100, '千': 1000, '万': 10000, '亿': 100000000, '壹': 1, '贰': 2, '叁': 3, '肆': 4, '伍': 5, '陆': 6,
                                '柒': 7, '捌': 8, '玖': 9, '拾': 10, '佰':100, '仟': 1000, '萬': 10000}
    common_used_numerals = {}
    for key in common_used_numerals_tmp:
        common_used_numerals[key] = common_used_numerals_tmp[key]

    total = 0
    r = 1  # 表示单位：个十百千...
    for i in range(len(uchars_chinese) - 1, -1, -1):
        val = common_used_numerals.get(uchars_chinese[i])
        if val >= 10 and i == 0:  # 应对 十三 十四 十*之类
            if val > r:
                r = val
                total = total + val
            else:
                r = r * val
                # total =total + r * x
        elif val >= 10:
            if val > r:
                r = val
            else:
                r = r * val
        else:
            total = total + r * val
    return total
def changeTimetoInt(str):
    if str.count("死刑") > 0 or str.count("无期") > 0 or str.count("终身"):
        return "-1"
    else:
        str = changeChineseNumToArab(str)
        nian = getone(str, r"\d+年").replace("年", "")
        if nian == "":
            nian = "0"
        yue = getone(str, r"\d+(个月|月)").replace("月", "").replace("个", "")
        if yue != "":
            nian = nian + "," + yue
        re = ""
        for n in nian:
            re = re + SBC2DBC(n)
        return re

#定位被告人信息,
def get_bgrxx_texts(zw):
    texts_bg = getall(zw, r'\s((|原审)上诉人(|（原审被告人）|（原审申诉人）)|(|原审)被告(|人))'
                          r'\w+(|＊＊|＊|××|×)\w{0,6}\S{0,1}'
                          r'(\w\S*因\w\S+被\w\S+|(|\w\S+)(男|女)(\S{0,2}\w\S*|))')#2018.10.16
    t_bg = []
    names_bg = []
    for text in texts_bg:
        name_bg=del_opt(getone(text.group(), r'((|原审)上诉人（原审被告人）|(|原审)被告(|人))\w+(|＊＊|＊|××|×)\w{0,6}'),
                        ["（","）","上诉人","原审","被告人","被告"])
        if name_bg == "":
            name_bg=del_opt(getone(text.group(), r"被告人：\w+?，"), ["被告人：", "，"])
        add = True
        if names_bg.count(name_bg) > 0 or name_bg.count("及其") > 0:
            add = False
        if add:
            names_bg.append(name_bg)
            t_bg.append(text.group())

    return t_bg
#定位犯罪事实
def get_fzss(zw):
    details_slcm = getone(zw, r'(|经)(|依法)审理查明\w*(|\S)(|.)(|\S\w*\S)\S*\w\S+.(|\S\w*\S)\S*\w\S+')  # .是为了匹配回车
    details_gsjg = getone(zw, r'((公诉机关|检察院)\w*指控\w*|原审认定|原判认定)(\S|)(.|)(\S\w*\S|)\S*\w\S+.(\S\w*\S|)\S*\w\S+')
    details_bg = details_slcm
    if details_bg == "" or ((details_bg.count("指控") or details_bg.count("原审") or details_bg.count("原判")) and details_bg.count("事实")):
        details_bg = details_gsjg
    if details_bg == "":
        details_bg = details_slcm
    return details_bg
#审判程序
def get_spcx(spzh):
    spcx = '1'
    sfzs = False
    if spzh.count("初") < 1:
        spcx = '2'
    if spzh.count("终") > 0:
        sfzs = True
    return spcx, sfzs
#文书类型ID
def get_wslx_id(dic_wslx,txt):
    if txt in dic_wslx:
        wslx_id=dic_wslx[txt]
    else:
        wslx_id=0
    return wslx_id
#拿到省市县id,2019.3.16update
def get_sheng_shi_xian_id(loc, dic_xian, dic_shi, dic_sheng, wsddshi_id=None, wsddxian_id=None):#dic_xian是['襄阳市襄城区':'421401','平安县':'130532'.......]
    xian_id, shi_id, sheng_id = "", "", ""
    if loc!='':
        for sheng in dic_sheng.keys():#遍历省
            if loc.count(sheng):
                sheng_id=dic_sheng[sheng]
        for shi in dic_shi.keys():
            if loc.count(shi):
                shi_id=dic_shi[shi]
        for xian in dic_xian.keys():
            if loc.count(xian):
                xian_id=dic_xian[xian]
        if sheng_id=='' and shi_id=='' and xian_id!='':#没有省，市
            sheng_id=xian_id[:2]
            shi_id=xian_id[:4]
        if sheng_id=='' and shi_id!='' and xian_id=='':#没有省，县
            sheng_id=shi_id[:2]
        if sheng_id=='' and shi_id!='' and xian_id!='':#没有省
            sheng_id=shi_id[:2]
        if sheng_id!='' and shi_id=='' and xian_id!='':#没有市
            shi_id=xian_id[:4]
        if shi_id=='' and loc.count('本市') and wsddshi_id!='' and wsddshi_id!=None:
            shi_id=wsddshi_id
            if sheng_id=='': sheng_id=shi_id[:2]
        if xian_id=='' and loc.count('本县') and wsddxian_id!='' and wsddxian_id!=None:
            xian_id=wsddxian_id
            if shi_id=='' and sheng_id=='':
                shi_id=wsddxian_id[:4]
                sheng_id=wsddxian_id[:2]
    else:
        xian_id, shi_id, sheng_id= "", "", ""
    return xian_id, shi_id, sheng_id
#罪名ID
def get_zmids(str, dic):
    re = '0'
    if str != "":
        if str in dic:
            re = dic[str]
        else:
            for s in dic.keys():
                if s.count(str) > 0 or str.count(s) > 0:
                    re = dic[s]
                    break
            if re == '0':
                for s in dic.keys():
                    k = True
                    for w in str:
                        if s.count(w) == 0:
                            k = False
                            break
                    if k:
                        re = dic[s]
                        break
    return re
def get_temp_zw(zw):
    details_slcm = getone(zw, r'(|经)(|依法)审理查明\w*(|\S)(|.)(|\S\w*\S)\S*\w\S+.(|\S\w*\S)\S*\w\S+')  # .是为了匹配回车
    details_gsjg = getone(zw, r'((公诉机关|检察院)\w*指控\w*|原审认定|原判认定)(\S|)(.|)(\S\w*\S|)\S*\w\S+.(\S\w*\S|)\S*\w\S+')
    if details_gsjg!="":
        fzss_temp=details_gsjg
    else:
        fzss_temp=details_slcm
    try:
        rules=r".+?"+fzss_temp[:15]#防治fzss_temp中有特殊符号*、.等等
        temp_zw=getone(zw, rules).replace(fzss_temp,"")
    except:
        temp_zw=zw
    return temp_zw

#去掉跟公司地点相关的，保证是出生地
def get_real_hj_text(texts, rules):
    rete = getone(texts, rules)
    if rete.count("罪") < 1 and rete.count("公司") < 1 and rete.count("年") < 1 and rete.count("月") < 1 \
            and rete.count("日") < 1 and rete.count("被") < 1 and rete.count("经") < 1:
        rete = rete.replace("生于", "")
    else:
        rete = ""
    return rete
#解析被告人信息

def get_bgrxxs(zw, dic_gz):
    bgrxxs = get_bgrxx_texts(get_temp_zw(zw))#拿到临时性正文再进行解析

    xm_bg,xb_bg,csrq_bg,mz_bg,jycd_bg,gz_bg,hj_bg,jzd_bg,dbjg_bg,dbrq_bg=[], [], [], [], [], [], [], [], [], []

    jobs = "无职业|无固定职业"
    for gz in dic_gz.keys():
        jobs = jobs + "|" + gz
    #户籍地、出生地规则
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
    #2018.10.15更新
    for bgrxx in bgrxxs:
        #姓名
        name=del_opt(getone(bgrxx, r"被告人（反诉自诉人）\w+?(。|，)|"
                                    r"原审被告人（上诉人）\w+?(。|，)|"
                                    r"被告人\S\w+?(。|，)|"
                                    r"被告\w+?(。|，)|"
                                    r"被告人（自报）\w+?\S|"
                                    r"上诉人.+?(。|，)|"
                                    r"被告人\S\w+（\w+）?(。|，)|"
                                    r"被告人（暨附带民事诉讼被告人）\w+?(。|，)|"
                                    r"被告单位\w+?(。|，)"),
                     ["被告人","上诉人","：","，","被告","（自报）",";",":","）","（","原审","。","暨附带民事诉讼","单位","反诉","自诉人","申诉人"])
        if name == "":
            name=del_opt(getone(bgrxx, "被告人（自报）\w+?；|"
                                        "上诉人\w+?(。|，)|"
                                        "被告人：\w+?（\w+?：\w+?）|"
                                        "被告人（自报姓名）\w+?(。|，)|"
                                        "被告人（反诉原告人）\w+?(。|，)|"
                                        "被告\w+?（\w+?）|"
                                        "被告人：\w＊"),
                         ["原审被告人（上诉人）","。","，","上诉人","（","）","自报姓名","被告人","：","别名","反诉","原告人","又名","自报","；","被告","申诉人"])

        if name == "":
            name=del_opt(getone(bgrxx, r'((|原审)上诉人（原审被告人）|被告人)\w+(|＊＊|＊|××|×)\w{0,6}'),
                         ["（","）","上诉人","原审","被告人"])
        if len(name) > 4:
            name = name[:3]
        xm_bg.append(name)
        #性别
        if bgrxx.count("男"):
            xb_bg.append("1")
        elif bgrxx.count("女"):
            xb_bg.append("2")
        else:
            xb_bg.append("0")#默认值0
        #出生日期
        csrq=del_opt(getone(bgrxx, r'\d{2,4}年\w{0,10}(出生|生)|生于\d{2,4}年\w{0,10}'),
                     ["生于","出生","生"])
        try:
            birth_date = changeStrtoTime(csrq)
        except:
            birth_date = ""
        if birth_date=="":
            birth_date="1900-01-01"
        csrq_bg.append(birth_date)
        #民族
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
        mz_bg.append(get_mz(bgrxx, r'\w+?族'))
        #教育程度
        jycd_bg.append(getone(bgrxx, r'\w+文化|文化程度\w+|研究生|博士|硕士|大学本科|大学|本科|大本|大学专科'
                                       r'|大专|专科|中专|中技|高中|初中|小学|文盲|半文盲|职高|无文化')
                       .replace('无文化','文盲').replace("大本", "本科").replace("大学","本科")
                       .replace("大学本科","本科").replace("大学专科","大专").replace("专科","大专"))
        #工作
        gz_bg.append(getone(bgrxx, jobs).replace("无职业", "无业").replace("无固定职业", "无业"))
        #户籍地
        locofbirth = get_real_hj_text(bgrxx, hj_rules)
        if locofbirth != "":
            hj_bg.append(locofbirth)
        else:
            hj=del_opt(getone(bgrxx, r'户籍(|地|所在地)(|\S)\w+'),["户籍地","户籍","所在地","：","，",":"])
            hj_bg.append(hj)
        #居住地,为啥每次都是居住地
        jzd=del_opt(getone(bgrxx, r'住址：\w+|住址\w+|住\w+'),['住址：','住址',"住在","住"])
        jzd_bg.append(jzd)
        #逮捕机构
        dbjg=del_opt(getone(bgrxx, r'(被|由|到)\w+(|决定)(刑事拘留|取保候审|投案|逮捕|行政拘留|执行逮捕|监视|强制隔离|羁押)'),
                     ["被","刑事拘留","由","到","决定","取保候审","投案","逮捕","行政拘留","执行逮捕","监视","强制隔离","羁押",'依法','执行','处以','采取'])
        dbjg_bg.append(dbjg)
        #逮捕日期
        dbrq_txt = getone(bgrxx.replace("（", "").replace("）", "").replace('、', ""),
                          r'\d{2,4}年\d{1,2}月\d{1,2}(|日)(，|\w*?)(被|由|到|经)\w*?(|决定)(刑事拘留|取保候审|投案|逮捕|行政拘留|执行逮捕|监视|强制隔离|羁押|抓获|查获|依法传唤|通知到案|解回)|'
                          r'\d{2,4}年\d{1,2}月\d{1,2}(|日)被.*?(刑事拘留|拘留|取保候审|逮捕|行政拘留|监视|强制隔离|羁押|抓获)|'
                          r'\d{2,4}年\d{1,2}月\d{1,2}(|日)，(|被告人\w{0,3})因涉嫌\w+?被\w+?(刑事拘留|取保候审|投案|逮捕|行政拘留|执行逮捕|监视|强制隔离|羁押|抓获|查获)|'
                          r'\d{2,4}年\d{1,2}月\d{1,2}(|日)(刑事拘留|取保候审|逮捕|行政拘留|监视|强制隔离|羁押|抓获|投案)|'
                          r'\d{2,4}年\d{1,2}月\d{1,2}(|日)\w+?(刑事拘留|取保候审|逮捕|行政拘留|监视|强制隔离|羁押|抓获)|'
                          r'\d{2,4}年\d{1,2}月\d{1,2}(|日)\w+?暂缓投劳')
        dbrq_Chinese = getone(dbrq_txt, r'\d{2,4}年\d{1,2}月\d{1,2}(|日)')
        try:
            dbrq = changeStrtoTime(dbrq_Chinese)
        except:
            dbrq = "1900-01-01"
        if dbrq == "":
            dbrq = "1900-01-01"
        dbrq_bg.append(dbrq)
    return xm_bg, xb_bg, csrq_bg, mz_bg, jycd_bg, gz_bg, hj_bg, jzd_bg, dbjg_bg, dbrq_bg
#拿到判决结果
def get_pjjgs(zw):
    temp=del_opt(getone(zw, r'(依照|依据)《中华人民共和国刑法》\w\S+(之|的)'),["依照","之","的"])
    fltw = getlaws(temp)
    zm,xflb,xq,hx, xqksrq, xqsfrq, bznx, fj = [],  [], [], [], [], [], [], []
    pjjg_text=getone(zw,r'(判(决|处)|裁定)如下(:|：|；|;)\s.+')
    pjjg_groups=getall(pjjg_text, r'(被告人|上诉人)(\w\S+?罪，判处\w\S*|\w\S*（\w{1,10}）\w\S*?罪，判处\w\S*|\w\S{0,10}(\w{1,10})\w\S*?罪，判处\w\S*|\w+?犯\S+?罪\S+)')
    for pjjg_group in pjjg_groups:
        line = pjjg_group.group()
        zm_onebgr_groups=getall(line,r'犯\S+?罪')
        zm_onebgr_list = []
        for zm_onebgr_group in zm_onebgr_groups:
            zm_onebgr = zm_onebgr_group.group().replace("犯", "")
            if zm_onebgr not in zm_onebgr_list:
                zm_onebgr_list.append(zm_onebgr)
        zm.append(zm_onebgr_list)
        if line.count("管制"):
            xflb.append("1")
        elif line.count("拘役"):
            xflb.append("2")
        elif line.count("有期徒刑"):
            xflb.append("3")
        elif line.count("无期徒刑"):
            xflb.append("4")
        elif line.count("死刑"):
            xflb.append("5")
        else:
            xflb.append("6")
        xq_longtime = getone(line, r'决定执行(管制|拘役|有期徒刑)\w+')
        if xq_longtime == "":
            xq_longtime = getone(line, r'管制|拘役|有期徒刑\w+')
        xq.append(changeTimetoInt(xq_longtime))
        hx.append(changeTimetoInt(getone(line, r'缓刑\w{1,10}')))
        bznx.append(changeTimetoInt(getone(line, r'剥夺政治权利\w{1,10}')))
        fj.append(changeMoneytoInt(getone(line, r'(没收|罚金|处罚)\w+')))
    times_cri = getall(zw, r'刑期\w\S+年\w+月\w+日\w+')
    for t_cri in times_cri:
        time_cri = t_cri.group()
        pattern = re.compile(r'\d+年\d+月\d+日')
        rete = pattern.findall(time_cri)
        if len(rete) > 1:
            xqksrq.append(changeStrtoTime(rete[0]))
            xqsfrq.append(changeStrtoTime(rete[-1]))
        else:
            xqksrq.append("")
            xqsfrq.append("")
    return fltw,zm,xflb,xq, hx, xqksrq,xqsfrq, bznx, fj
def getlaws(laws):
    laws = changeChineseNumToArab(laws)
    re = ""
    fa = "001"
    tiao = ""
    zhi = ""
    for law in getall(laws, r'(第\d+\w+|\d+款)'):
        l = law.group()
        if l.count("条") > 0:
            tiao = getone(l, r'\d+')
            while len(tiao) < 3:
                tiao = "0" + tiao
            zhi = getone(getone(l, r'\d+第'), r'\d+')
            while len(zhi) <2:
                zhi = "0" + zhi
            kuan = getone(getone(l, r'条第\d+|\d+款'), r'\d+')
            if kuan == "":
                kuan = getone(getone(l, r'\d+第\d+'), r'第\d+').replace("第", "")
            if kuan == "":
                kuan = "01"
            else:
                while len(kuan) < 2:
                    kuan = "0" + kuan
            re = re + fa+tiao+zhi+kuan + ","
        elif tiao != "" and zhi != "":
            kuan = getone(getone(l, r'\d+款'), r'\d+')
            if kuan == "":
                kuan = "01"
            else:
                while len(kuan) < 2:
                    kuan = "0" + kuan
            re = re + fa+tiao+zhi+kuan + ","

    return re[:-1]

#将byte的类型的list转换成正常的list
def byte_list2list(byte_list):
    for i in range(len(byte_list)):
        byte_list[i] = byte_list[i].decode()[1:-1].replace("'", "").replace(" ", "").split(",")
    return byte_list
def AorB(a,b):
    c=a
    if a==""and b!="":
        c = b
    if a=="0"and b!="0":
        c=b
    if a=="1900-01-01"and b!="1900-01-01":
        c=b
    return c
def SqlorRedis(list, byte_list):
    re_list=[]
    for i in range(len(list[0])):#每个被告人
        c=[]
        a=[x[i] for x in list]
        b=[x[i] for x in byte_list]
        for j in range(10):
            c.append(AorB(a[j],b[j]))
        re_list.append(c)
    return re_list
def get_guid(xm_bg,xb_bg,csrq_bg,mz_bg,ws_md5,wsddsheng,wsddshi):
    if xm_bg!="" and xb_bg!="0" and csrq_bg!="1900-01-01" and mz_bg!="" and wsddsheng!=''and wsddshi!='':
        guid = uuid.uuid3(uuid.NAMESPACE_DNS, name=xm_bg + repr(xb_bg) + csrq_bg + repr(mz_bg)+str(wsddsheng)+str(wsddshi))
    else:
        guid = uuid.uuid3(uuid.NAMESPACE_DNS, name=ws_md5 + xm_bg)
    guid = repr(guid).replace("UUID", "").replace("(", "").replace(")", "")
    return guid

#解析前科信息
def get_qkxx(text,dic_zm):#拿到前科判决日期，前科刑期，前科罪名，前科剥政年限，前科释放日期，前科罚金
    text=text.replace("l","1")
    qkzm=getone(text,r'犯\w+罪{1}|受贿罪|故意伤害罪|聚众斗殴罪|抢劫罪|开设赌场罪|贪污罪|绑架罪|贩卖毒品罪|强奸罪|盗窃罪|'
                     r'诈骗罪|寻衅滋事罪|非法经营罪|行贿罪|危险驾驶罪|破坏电力设备罪|挪用资金罪|敲诈勒索罪|销售赃物罪|'
                     r'故意杀人罪|妨害公务罪|非法持有毒品罪').replace("犯","")
    qkzm_id = get_zmids(qkzm, dic_zm)
    qkxq = changeTimetoInt(getone(text, r'判处\w+').replace("判处", ""))
    qksfrq_temp=getone(text, r'\w+?刑满释放|\w+?被释放|\w+?减刑释放|\w+?释放|刑期至\w+|\w+?被假释|缓刑\w+|有期徒刑\w+')
    text4qkpjrq=text.replace(qksfrq_temp,"")
    qkpjrq=getone(text4qkpjrq, r'\d{4}年\d{1,2}月\d{1,2}日|\d{4}年\d{1,2}月')
    if qkpjrq=="":
        qkpjrq = getone(text4qkpjrq, r'.{4}年\w{1,2}月\w{1,3}日|.{4}年\w{1,2}月')
    if qkpjrq == "":
        qkpjrq = getone(text4qkpjrq, r'\w{4}年\w{1,2}月|\w{4}年')
    if qkpjrq!=""and (not qkpjrq.count("日"))and (not qkpjrq.count("月")):
        qkpjrq=qkpjrq+"1月1日"
    elif qkpjrq!=""and (not qkpjrq.count("日")):
        qkpjrq=qkpjrq+"1日"
    qk_nian=getone(qkpjrq,r'\w{4}年')#为了应对“同年”
    qkpjrq=changeStrtoTime(qkpjrq)
    if qkpjrq=="":
        qkpjrq="1900-01-01"
    temp=getone(text, r'\w+?刑满释放|\w+?被释放|\w+?减刑释放|\w+?释放|刑期至\w+|\w+?被假释')
    temp=temp.replace('同年',qk_nian)
    qksfrq=getone(temp, r'\w{4}年\w+?月\w+?日|\w{4}年\w{1,2}月|\w{4}年')
    if qksfrq!="" and (not qksfrq.count("日"))and (not qksfrq.count("月")):
        qksfrq=qksfrq+"1月1日"
    if qksfrq!="" and (not qksfrq.count("日")):
        qksfrq=qksfrq+"1日"
    qksfrq=changeStrtoTime(qksfrq)

    if qksfrq == "":
        qksfrq = "1900-01-01"
    qkbz=changeTimetoInt(getone(text, r'剥夺政治权利\w+').replace("剥夺政治权利", ""))
    qkfj=changeMoneytoInt(getone(text, r'罚金\S+?元').replace("罚金", "").replace("，",""))
    return qkzm_id,qkpjrq,qksfrq,qkbz,qkxq,qkfj
#针对全部是逗号的前科信息进行解析
def get_all_comma_qk_sent(bgrxx):
    qkpjrq_list=get_all_text(bgrxx,r'\d{4}年\d{1,2}月\d{1,2}日因|\d{4}年\d{1,2}月因|\d{4}年因|'
                                   r'同年\d{1,2}月\d{1,2}日因|'
                                   r'同年\d{1,2}月因')
    if len(qkpjrq_list)==0:
        qkpjrq_list=get_all_text(bgrxx,r'因\w+?，{0,1}于\d{4}年\d{1,2}月\d{1,2}日|'
                                       r'因\w+?，{0,1}于\d{4}年\d{1,2}月|'
                                       r'因\w+?，{0,1}于\d{4}年')
    qk_sentence=[]
    if len(qkpjrq_list)>1:
        for i in range(len(qkpjrq_list)-1):
            temp=getone(bgrxx,qkpjrq_list[i]+r'.+?'+qkpjrq_list[i+1]).replace(qkpjrq_list[i+1],'')
            if temp.count("有期徒刑") or temp.count("拘役") or temp.count("管制"):#别忘了加判断
                qk_sentence.append(temp)
        temp=getone(bgrxx,qkpjrq_list[-1]+r'.+')
        if temp.count("有期徒刑") or temp.count("拘役") or temp.count("管制"):
            qk_sentence.append(temp)
    else:
        if bgrxx.count("有期徒刑") or bgrxx.count('拘役')or bgrxx.count('管制'):
            qk_sentence.append(bgrxx)
    return qk_sentence
#拿到被告人的多个前科text，然后get_qkxx进行解析
def get_qkxx_sentence(inf_txt_bg):
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
        if bgrxx.count("有期徒刑") or bgrxx.count("拘役") or bgrxx.count("管制"):
            qk_sentence.append(bgrxx)
    else:#多条
        if bgrxx.count('；')or bgrxx.count(';'):#有分号，正常用分号分割，然后append
            if bgrxx.count('；'):
                inf_bg_splits =bgrxx.split("；")
            else:
                inf_bg_splits =bgrxx.split(";")
            for inf_bg_split in inf_bg_splits:#行政拘留我们不算
                if inf_bg_split.count("有期徒刑") or inf_bg_split.count("拘役") or inf_bg_split.count("管制"):
                    qk_sentence.append(inf_bg_split)
        else:#没有分号
            find_stop=getone(bgrxx,r'.+?\d{4}年')
            if find_stop.count('。'):#可以用句号
                bgrxx_sent=getone(bgrxx,r'.+?。')
                bgrxx=bgrxx.replace(bgrxx_sent,'')#去掉第一句被告人信息
                if bgrxx.count('。'):#再看有没有句号,有的话直接用句号分
                    inf_bg_splits = bgrxx.split("。")
                    for inf_bg_split in inf_bg_splits:
                        if inf_bg_split.count("有期徒刑") or inf_bg_split.count("拘役") or inf_bg_split.count("管制"):
                            qk_sentence.append(inf_bg_split)
                else:#没有句号，全是逗号，我们用xxx年xx月xx日因来划分
                    qk_sentence=get_all_comma_qk_sent(bgrxx)
            else:#如果第一句都没有句号
                if bgrxx.count('。'):#第一句没有句号，但是后边的有，我们直接split句号
                    inf_bg_splits = bgrxx.split("。")
                    for inf_bg_split in inf_bg_splits:
                        if inf_bg_split.count("有期徒刑") or inf_bg_split.count("拘役") or inf_bg_split.count("管制"):
                            qk_sentence.append(inf_bg_split)
                else:#没有句号，全是逗号
                    qk_sentence= get_all_comma_qk_sent(bgrxx)
    return qk_sentence

#拿到文书内容相关
def get_wsnr(zw,dic_xian, dic_shi, dic_sheng):
    #初审字号
    ajzh_chushen = getone(zw, r'\S\d{4}\S\w+初(字|)\w+号')
    #当前字号
    ajzh_dangqian=getone(zw, r'(\S|（)\d{4}(\S|）)\w+(字|)\w+号')#注意这两个在初审的时候一致，终审的时候不一致
    texts_bg = get_bgrxx_texts(get_temp_zw(zw))
    name_bg=[]
    for text_bg in texts_bg:
        temp=getone(text_bg, r'((|原审)上诉人（原审被告人）|被告人)\w+(|＊＊|＊|××|×)\w{0,6}')
        temp=del_opt(temp, ["（", "）", "上诉人", "原审", "被告人"])
        name_bg.append(temp)
    bgrsl=len(name_bg)
    spjg = getone(zw, r'\w{2,30}院')#审判机构
    #原告人
    ygr = del_opt(getone(getone(zw, r"原告人\w+?(|＊＊|＊|××|×)(向|和|与|已|及其|提起|等|到|申请)"), r"原告人\S+"),
                  ["原告人","向","和","与","已","及其","提起","等","到","申请"])

    if ygr == "":
        ygr = getone(getone(zw, r"原告人\w+\S"), r"原告人\w+").replace("原告人", "")
    if len(ygr) > 4:
        ygr = ""
    #检察员
    jcy=del_opt(getone(zw, r'检察员\w+(并|依法|出庭|、|，)'),
                ["检察员","，","、","依法","出庭","支持公诉","并","监督庭审活动"])

    #辩护人
    bhur=del_opt(getone(zw, r"辩护人(|暨委托代理人|暨诉讼代理人)\S{0,1}(\w{2,4}|\w{1,2}(|＊＊|＊|××|×))(，|、|。)"),
                 ["辩护人","，","、","。","：",":","暨委托代理人","暨诉讼代理人"])
    #起诉机构
    qsjg=del_opt(getone(zw, r'((公|抗)诉机关|)(\S原公诉机关\S|)\w+(（\w+）|)\w*检察(院|)'),
                 ["(原公诉机关)","（原公诉机关）","公诉机关","抗诉机关"])
    qsjg_replace = getone(qsjg, "（\w+）")
    if len(qsjg_replace) > 0:
        qsjg = qsjg.replace(qsjg_replace, "")
    #起诉字号
    qszh = getone(zw, r'以\w+(\S\d{4}\S|)\w*号').replace("以", "")
    #犯罪时间,首先定位犯罪事实
    details_slcm = getone(zw, r'(|经)(|依法)审理查明\w*(|\S)(|.)(|\S\w*\S)\S*\w\S+.(|\S\w*\S)\S*\w\S+')  # .是为了匹配回车
    details_gsjg = getone(zw, r'((公诉机关|检察院)\w*指控\w*|原审认定|原判认定)(\S|)(.|)(\S\w*\S|)\S*\w\S+.(\S\w*\S|)\S*\w\S+')
    details_bg = details_slcm
    if details_bg == "" or (
            (details_bg.count("指控") or details_bg.count("原审") or details_bg.count("原判")) and details_bg.count("事实")):
        details_bg = details_gsjg
    if details_bg == "":
        details_bg = details_slcm
    time_detailes = getone(details_bg, r'\w{4}年(\w{1,2}月|)(\w{1,3}日|)')
    if time_detailes == "":
        time_detailes = getone(details_gsjg, r'\w{4}年(\w{1,2}月|)(\w{1,2}日|)')
    if time_detailes.count("月") == 0:
        time_detailes = time_detailes + "1月1日"
    elif time_detailes.count("日") == 0:
        time_detailes = time_detailes + "1日"
    fzsj = changeStrtoTime(time_detailes)
    if fzsj == "":
        fzsj = "1900-01-01"#插入的时候，加引号即可，跟strgeshi一致
    #犯罪地点，省市县明细，因为是CRF,所以此处不赋值
    #犯罪原因，CRF
    #作案工具，CRF

    case=get_fzss(zw)
    # 涉案金额，str类型
    money_txt = getone(case, r"\d+(|,|\.)\d*(|余|多|万|余万|万余|千|千余)元").replace(",", "").replace("余元", "") \
        .replace("多元", "").replace("元", "").replace("，", "")
    saje=changeMoneytoInt(money_txt)
    #审判长,审判员，陪审员，书记员
    spz=getone(getone(zw, r'审\s*?判\s*?长\s*?(\w+?\s*?\w+?\s*?\w+?|\w+?\s*?\w+?|\w+?)').replace("　", ""), r'审判长\w+')\
        .replace("审判长", "")
    spy=getone(getone(zw, r'审\s*?判\s*?员\s*?(\w+?\s*?\w+?\s*?\w+?|\w+?\s*?\w+?|\w+?)').replace("　", ""), r'审判员\w+')\
        .replace("审判员", "")
    psy=getone(getone(zw, r'陪s*?审s*?员\s*?(\w+?\s*?\w+?\s*?\w+?|\w+?\s*?\w+?|\w+?)').replace("　", ""), r'陪审员\w+')\
        .replace("陪审员", "")
    sjy = getone(getone(zw, r'书\s*?记\s*?员\s*?(\w+?\s*?\w+?\s*?\w+?|\w+?\s*?\w+?|\w+?)').replace("　", ""),r'书记员\w+')\
        .replace("书记员", "")
    #判决日期
    pjrq = ""
    pattern = re.compile(r'\w{4}年\w{1,2}月\w{1,3}日')
    rete = pattern.findall(zw)
    if len(rete) > 0:
        pjrq = changeStrtoTime(rete[-1])
    if pjrq == "":
        pjrq = "1900-01-01"
    #审判程序，是否终审，是否终审要str一下
    spcx, sfzs = get_spcx(ajzh_dangqian)
    #文书地点，省市县，根据审判机构（法院）来判定
    loc_ws = getone(spjg, r'\w+(区|县|旗|市|盟|州)')
    wsddxian, wsddshi, wsddsheng = get_sheng_shi_xian_id(loc_ws, dic_xian, dic_shi, dic_sheng)

    #把犯罪地点暂时按照文书地点来弄
    fzddsheng=wsddsheng
    fzddshi=wsddshi
    fzddxian=wsddxian
    #首部
    shoubu= str(get_bgrxx_texts(zw)).replace("'", ";")[2:-2]
    return ajzh_chushen,ajzh_dangqian,bgrsl,spjg,ygr,jcy,bhur,qsjg,qszh,fzsj,fzddsheng,fzddshi,fzddxian,saje,spz,spy,psy,sjy,pjrq,spcx,sfzs,wsddsheng,wsddshi,wsddxian,case,shoubu
def buqi_opt(dbjg_bg,dbrq_bg,zm,xflb,xq,hx,bznx,fj,bgrsl):
    while len(dbjg_bg) < bgrsl:
        dbjg_bg.append("")
    while len(dbrq_bg) < bgrsl:
        dbrq_bg.append("1900-1-1")
    while len(zm) < bgrsl:
        zm.append([])
    while len(xflb) < bgrsl:
        xflb.append("6")
    while len(xq) < bgrsl:
        xq.append("0")
    while len(hx) < bgrsl:
        hx.append("0")
    while len(bznx) < bgrsl:  # 剥政年限
        bznx.append("0")
    while len(fj) < bgrsl:
        fj.append("0")
