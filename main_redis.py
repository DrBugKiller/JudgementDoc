from someTool import *
import psycopg2
import threading
import redis

input_list=[]

def inset_lianhe(roung_id, bgrspb_xx_table_name,qkxx_table_name):
    global index
    for round in range(33):#600除以线程数量
        cur.execute("select wsid,zw,wsmc,wslx,ay,cpjg from ws_json_enter where id>=%s and id<%s" % ((round+roung_id*33) * 10000, (round+1+roung_id*33) * 10000))
        ws_jsons=cur.fetchall()
        for ws_json in ws_jsons:#ws_json[0,1,2,3,4,5]分别是md5,正文，文书名称，文书类型,案由,裁判结果
           temp_zw=ws_json[1].replace("&ldquo;","").replace("&times","X").replace("&rdquo;","")#删除特殊符号
           wslx=get_wslx_id(dic_wslx,ws_json[3])#文书类型
           liyou=get_zmids(ws_json[4],dic_zm)#理由，罪名
           #文书相关字段，注意bgrsl是int类型，insert的时候需要转换成str，最好用repr(bgrsl)
           ajzh,spzh,bgrsl,spjg,ygr,jcy,bhur,qsjg,qszh,fzsj,fzddsheng,fzddshi,fzddxian,saje,spz,spy,psy,sjy,\
           pjrq,spcx,sfzs,wsddsheng,wsddshi,wsddxian,fzss,shoubu=get_wsnr(temp_zw,dic_xian,dic_shi,dic_sheng)
           #被告人信息相关
           xm_bg,xb_bg,csrq_bg,mz_bg,jycd_bg,gz_bg,hj_bg,jzd_bg,dbjg_bg,dbrq_bg=get_bgrxxs(temp_zw, dic_gz)
           # 用终审补充初审，判断是否为初审、终审，初审，正常插入，终审，根据初审字号和姓名update
           bgrxx_list=[xm_bg,xb_bg,csrq_bg,mz_bg,jycd_bg,gz_bg,hj_bg,jzd_bg,dbjg_bg,dbrq_bg]
           ajzh_chushen,ajzh_dangqian=ajzh,spzh
           #如果是初审
           if ajzh_chushen == ajzh_dangqian:
               xingchu_ah_list.append(ajzh_chushen)
               r.hmset(ajzh_chushen,{"xm_bg":xm_bg,"xb_bg":xb_bg,"csrq_bg":csrq_bg,"mz_bg":mz_bg,
                                     "jycd_bg":jycd_bg,"gz_bg":gz_bg,"hj_bg":hj_bg,"jzd_bg":jzd_bg,
                                     "dbjg_bg":dbjg_bg,"dbrq_bg":dbrq_bg})
               #看一下此处的户籍地是否是文本还是代号
               def normal_insert():#ws_json[1]:正文；bgrsl:被告人数量；hj_bg[i]：户籍地
                   global index
                   fltw, zm, xflb, xq, hx, xqksrq, xqsfrq, bznx, fj = get_pjjgs(temp_zw)#审判结果
                   buqi_opt(dbjg_bg,dbrq_bg,zm,xflb,xq,hx,bznx,fj,bgrsl)#被告人数量补齐操作
                   k = 0  #用来补齐刑期开始时间和刑期释放时期
                   # 被告人信息段
                   inf_txt_bg = get_bgrxx_texts(get_temp_zw(temp_zw))#去掉审判结果
                   if len(inf_txt_bg) != 0:
                       for i in range(bgrsl):
                           zmid_list = [get_zmids(x, dic_zm) for x in zm[i]]# 2018.10.20更新罪名：
                           zmid_str = str(zmid_list).replace("[", "").replace("]","").replace("'","")
                           bgrhjxian, bgrhjshi, bgrhjsheng = get_sheng_shi_xian_id(hj_bg[i], dic_xian, dic_shi, dic_sheng,wsddshi,wsddxian)
                           bgrjzdxian, bgrjzdshi, bgrjzdsheng = get_sheng_shi_xian_id(jzd_bg[i], dic_xian, dic_shi, dic_sheng,wsddshi,wsddxian)
                           bgrjzdxj = jzd_bg[i].replace(bgrjzdxian, "").replace(bgrjzdshi, "").replace(bgrjzdsheng,"")
                           guid=get_guid(xm_bg[i],xb_bg[i],csrq_bg[i],mz_bg[i],ws_json[0],wsddsheng,wsddshi)#姓名，性别，出生日期，民族，省市
                           qk_sentence = get_qkxx_sentence(inf_txt_bg[i])
                           #先加一个判断是否是不准确的前科信息,前科信息不能超过2018年
                           if qk_sentence!=[]:
                               qkzm_id,qkpjrq, qksfrq, qkbz, qkxq, qkfj = get_qkxx(qk_sentence[-1],dic_zm)
                               try:
                                   if int(qksfrq[:4]) > 2018: qk_sentence = qk_sentence[:-1]
                               except:
                                   pass
                           qksl = len(qk_sentence)
                           # 前科数量取到
                           # 2018.11.3前科信息也要插入到总表bgrspb_xx_test中,我们此处拿到最后一次的前科信息
                           if qksl > 0: qkzm_id, qkpjrq, qksfrq, qkbz, qkxq, qkfj = get_qkxx(qk_sentence[-1],dic_zm)#拿到最后一次

                           else: qkzm_id, qkpjrq, qksfrq, qkbz, qkxq, qkfj = "", "1900-01-01", "1900-01-01", "", "", ""

                           # 被告人信息完毕，因为刑期开始和释放日期长度不一致，要处理一下
                           if 0 < int(xflb[i]) < 4 and hx[i] == "0" and k < len(xqksrq) and k < len(xqsfrq):
                               if xqksrq[k] != "" and xqsfrq[k] != "":
                                   insert_bgrspb_xx = "insert into " + bgrspb_xx_table_name + "(bgrguid,bgrxm,bgrxb,bgrcsrq,bgrmz,bgrjycd,bgrgz," \
                                                      "bgrhjsheng,bgrhjshi,bgrhjxian,bgrjzdsheng,bgrjzdshi,bgrjzdxian,bgrjzdxj," \
                                                      "dbrq,dbjg,fltw,zm,xflb,xq,hx,xqksrq,xqsfrq,bznx,fj,zw,wsmd5,ajzh,qksl," \
                                                      "cszh,bgrsl,spjg,ygr,jcy,bhur,qsjg,qszh,fzsj,fzddsheng,fzddshi,fzddxian," \
                                                      "saje,spz,spy,psy,sjy,pjrq,spcx,sfzs,wsddsheng,wsddshi,wsddxian,wslx,fzss,liyou,pjjg," \
                                                      "wsmc,shoubu,qkzm_id,qkpjrq,qksfrq,qkbz,qkxq,qkfj) " \
                                                      "values(%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                                                      "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                                                      "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                                                      "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" \
                                                      % (guid, xm_bg[i], str(xb_bg[i]), csrq_bg[i],getids(mz_bg[i], dic_mz),str(getids(jycd_bg[i], dic_jycd)),
                                                         getids(gz_bg[i], dic_gz), bgrhjsheng, bgrhjshi, bgrhjxian,bgrjzdsheng, bgrjzdshi, bgrjzdxian, bgrjzdxj,
                                                         dbrq_bg[i],dbjg_bg[i], fltw, zmid_str, xflb[i], xq[i], hx[i],xqksrq[k],xqsfrq[k], bznx[i], str(fj[i]),
                                                         temp_zw, ws_json[0],spzh,str(qksl), ajzh, str(bgrsl), spjg, ygr, jcy, bhur, qsjg,qszh,fzsj, fzddsheng,
                                                         fzddshi, fzddxian, saje, spz, spy, psy,sjy,pjrq, spcx, repr(sfzs), wsddsheng, wsddshi, wsddxian, wslx,
                                                         fzss, liyou, ws_json[5], ws_json[2], shoubu, qkzm_id,qkpjrq, qksfrq, qkbz,qkxq, str(qkfj))
                               else:
                                   # 这个insert少了刑期开始日期，刑期释放日期
                                   insert_bgrspb_xx = "insert into " + bgrspb_xx_table_name + "(bgrguid,bgrxm,bgrxb,bgrcsrq,bgrmz,bgrjycd,bgrgz," \
                                                      "bgrhjsheng,bgrhjshi,bgrhjxian,bgrjzdsheng,bgrjzdshi,bgrjzdxian,bgrjzdxj,dbrq,dbjg,fltw,zm," \
                                                      "xflb,xq,hx,bznx,fj,zw,wsmd5,ajzh,qksl,cszh,bgrsl,spjg,ygr,jcy,bhur,qsjg,qszh,fzsj,fzddsheng," \
                                                      "fzddshi,fzddxian,saje,spz,spy,psy,sjy,pjrq,spcx,sfzs,wsddsheng,wsddshi,wsddxian,wslx,fzss,liyou," \
                                                      "pjjg,wsmc,shoubu,qkzm_id,qkpjrq,qksfrq,qkbz,qkxq,qkfj) " \
                                                      "values(%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                                                      "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                                                      "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                                                      "'%s','%s','%s','%s','%s','%s')" \
                                                      % (guid, xm_bg[i], str(xb_bg[i]), csrq_bg[i],getids(mz_bg[i], dic_mz),str(getids(jycd_bg[i], dic_jycd)),
                                                         getids(gz_bg[i], dic_gz), bgrhjsheng, bgrhjshi, bgrhjxian, bgrjzdsheng, bgrjzdshi, bgrjzdxian, bgrjzdxj,
                                                         dbrq_bg[i],dbjg_bg[i], fltw, zmid_str, xflb[i], xq[i], hx[i], bznx[i], str(fj[i]), temp_zw, ws_json[0],
                                                         spzh, str(qksl), ajzh,str(bgrsl), spjg, ygr, jcy, bhur, qsjg, qszh, fzsj, fzddsheng,fzddshi, fzddxian,
                                                         saje, spz, spy, psy, sjy, pjrq, spcx,repr(sfzs), wsddsheng, wsddshi, wsddxian, wslx, fzss,liyou,ws_json[5],
                                                         ws_json[2], shoubu, qkzm_id, qkpjrq, qksfrq,qkbz, qkxq,str(qkfj))
                               k += 1
                           else:
                               insert_bgrspb_xx = "insert into " + bgrspb_xx_table_name + "(bgrguid,bgrxm,bgrxb,bgrcsrq,bgrmz,bgrjycd,bgrgz," \
                                                  "bgrhjsheng,bgrhjshi,bgrhjxian,bgrjzdsheng,bgrjzdshi,bgrjzdxian,bgrjzdxj,dbrq,dbjg,fltw,zm," \
                                                  "xflb,xq,hx,bznx,fj,zw,wsmd5,ajzh,qksl,cszh,bgrsl,spjg,ygr,jcy,bhur,qsjg,qszh,fzsj,fzddsheng," \
                                                  "fzddshi,fzddxian,saje,spz,spy,psy,sjy,pjrq,spcx,sfzs,wsddsheng,wsddshi,wsddxian,wslx,fzss,liyou," \
                                                  "pjjg,wsmc,shoubu,qkzm_id, qkpjrq, qksfrq, qkbz, qkxq, qkfj) " \
                                                  "values(%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                                                  "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                                                  "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                                                  "'%s','%s','%s','%s','%s','%s')" \
                                                  % (guid, xm_bg[i],str(xb_bg[i]),csrq_bg[i],getids(mz_bg[i],dic_mz),str(getids(jycd_bg[i],dic_jycd)),
                                                     getids(gz_bg[i],dic_gz),bgrhjsheng, bgrhjshi, bgrhjxian, bgrjzdsheng, bgrjzdshi,bgrjzdxian,bgrjzdxj,
                                                     dbrq_bg[i], dbjg_bg[i], fltw, zmid_str, xflb[i],xq[i],hx[i], bznx[i], str(fj[i]), temp_zw, ws_json[0],
                                                     spzh,str(qksl),ajzh, str(bgrsl), spjg, ygr, jcy, bhur, qsjg, qszh, fzsj,fzddsheng,fzddshi, fzddxian,
                                                     saje,spz,spy,psy,sjy,pjrq,spcx,repr(sfzs),wsddsheng,wsddshi,wsddxian,wslx,fzss,liyou,ws_json[5],
                                                     ws_json[2],shoubu,qkzm_id,qkpjrq,qksfrq,qkbz,qkxq,str(qkfj))
                           try:
                               cur.execute(insert_bgrspb_xx)
                           except Exception as e:
                               print(e)
                           j = 0  # 第三层循环，以这个人的信息inf_bg[i]这一段作为输入，拿到前科罪名，前科判决信息，前科刑期，前科剥政年限，前科罚金
                           while j < qksl:
                               qkzm_id, qkpjrq, qksfrq, qkbz, qkxq, qkfj = get_qkxx(qk_sentence[j],dic_zm)
                               try:
                                   cur.execute("insert into "+qkxx_table_name+"(qkzm,qkpjrq,qkxq,qkbznx,qkfj,qksfrq,bgrguid,ajzh,wsmd5,qklx)"
                                               "values('%s','%s','%s','%s','%s','%s',%s,'%s','%s','%s')"
                                               % (qkzm_id,qkpjrq,qkxq,qkbz,str(qkfj),qksfrq,guid,spzh,ws_json[0],str(1)))
                                   print("插入前科成功")
                               except Exception as e:
                                   print(e)
                               j += 1
                           index += 1
                           print(index)
               normal_insert()
           else:  # 说明不是初审
               if ajzh_chushen in xingchu_ah_list:#已经插入过初审案件,开始更新
                    redis_bgrxx=r.hmget(ajzh_chushen,"xm_bg","xb_bg","csrq_bg","mz_bg","jycd_bg",
                                        "gz_bg","hj_bg","jzd_bg","dbjg_bg","dbrq_bg")
                    #0:姓名，1：性别,2:出生日期，3：民族，4：教育程度，5：工作，6：户籍，7：居住地，8：逮捕机构，9：逮捕日期
                    redis_bgrxx=byte_list2list(redis_bgrxx)
                    if len(bgrxx_list[0])==len(redis_bgrxx[0]):#如果提取的被告人数量一致：
                        bgrxx_list=SqlorRedis(bgrxx_list,redis_bgrxx)
                        for bgr_i in range(len(bgrxx_list)):
                            bgrhjxian, bgrhjshi, bgrhjsheng = get_sheng_shi_xian_id(bgrxx_list[bgr_i][6], dic_xian, dic_shi, dic_sheng,wsddshi,wsddxian)#户籍地省市县要相互补充
                            bgrjzdxian, bgrjzdshi, bgrjzdsheng = get_sheng_shi_xian_id(bgrxx_list[bgr_i][7], dic_xian, dic_shi, dic_sheng,wsddshi,wsddxian)#居住地也要相互补充
                            bgrjzdxj = bgrxx_list[bgr_i][7].replace(bgrjzdxian, "").replace(bgrjzdshi, "").replace(bgrjzdsheng,"")
                            try:
                                cur.execute("UPDATE "+bgrspb_xx_table_name+" SET "
                                        "bgrxm='%s',bgrxb='%s',bgrcsrq='%s',bgrmz='%s',bgrjycd='%s',bgrgz='%s',bgrhjsheng='%s',"
                                        "bgrhjshi='%s',bgrhjxian='%s',bgrjzdsheng='%s',bgrjzdshi='%s',bgrjzdxian='%s',"
                                        "bgrjzdxj='%s',dbjg='%s',dbrq='%s' WHERE ajzh='%s' and bgrxm='%s'"
                                        %(bgrxx_list[bgr_i][0],getids(bgrxx_list[bgr_i][1],dic_xb),str(bgrxx_list[bgr_i][2]),
                                          getids(bgrxx_list[bgr_i][3],dic_mz),getids(str(bgrxx_list[bgr_i][4]),dic_jycd),
                                          getids(bgrxx_list[bgr_i][5],dic_gz),bgrhjsheng,bgrhjshi,bgrhjxian,bgrjzdsheng,bgrjzdshi,
                                          bgrjzdxian,bgrjzdxj,bgrxx_list[bgr_i][8],bgrxx_list[bgr_i][9],ajzh_chushen,bgrxx_list[bgr_i][0]))
                                print("update successfully!")
                            except:
                                print("update error!")
               #正常插终审内容。。。
               def normal_insert():#ws_json[1]:正文；bgrsl:被告人数量；hj_bg[i]：户籍地
                   global index
                   fltw, zm, xflb, xq, hx, xqksrq, xqsfrq, bznx, fj = get_pjjgs(temp_zw)#审判结果
                   buqi_opt(dbjg_bg,dbrq_bg,zm,xflb,xq,hx,bznx,fj,bgrsl)#被告人数量补齐操作
                   k = 0  #用来补齐刑期开始时间和刑期释放时期
                   # 被告人信息段
                   inf_txt_bg = get_bgrxx_texts(get_temp_zw(temp_zw))#去掉审判结果
                   if len(inf_txt_bg) != 0:
                       for i in range(bgrsl):
                           # 2018.10.20更新罪名：
                           zmid_list = [get_zmids(x, dic_zm) for x in zm[i]]
                           zmid_str = str(zmid_list).replace("[", "").replace("]","").replace("'","")
                           bgrhjxian, bgrhjshi, bgrhjsheng = get_sheng_shi_xian_id(hj_bg[i], dic_xian, dic_shi, dic_sheng,wsddshi,wsddxian)
                           bgrjzdxian, bgrjzdshi, bgrjzdsheng = get_sheng_shi_xian_id(jzd_bg[i], dic_xian, dic_shi, dic_sheng,wsddshi,wsddxian)
                           bgrjzdxj = jzd_bg[i].replace(bgrjzdxian, "").replace(bgrjzdshi, "").replace(bgrjzdsheng,"")
                           guid=get_guid(xm_bg[i],xb_bg[i],csrq_bg[i],mz_bg[i],ws_json[0],wsddsheng,wsddshi)#姓名，性别，出生日期，民族，省市
                           qk_sentence = get_qkxx_sentence(inf_txt_bg[i])
                           #先加一个判断是否是不准确的前科信息,前科信息不能超过2018年
                           if qk_sentence!=[]:
                               qkzm_id,qkpjrq, qksfrq, qkbz, qkxq, qkfj = get_qkxx(qk_sentence[-1],dic_zm)
                               try:
                                   if int(qksfrq[:4]) > 2018: qk_sentence = qk_sentence[:-1]
                               except:
                                   pass
                           qksl = len(qk_sentence)
                           # 前科数量取到
                           # 2018.11.3前科信息也要插入到总表bgrspb_xx_test中,我们此处拿到最后一次的前科信息
                           if qksl > 0: qkzm_id, qkpjrq, qksfrq, qkbz, qkxq, qkfj = get_qkxx(qk_sentence[-1],dic_zm)#拿到最后一次

                           else: qkzm_id, qkpjrq, qksfrq, qkbz, qkxq, qkfj = "", "1900-01-01", "1900-01-01", "", "", ""

                           # 被告人信息完毕，因为刑期开始和释放日期长度不一致，要处理一下
                           if 0 < int(xflb[i]) < 4 and hx[i] == "0" and k < len(xqksrq) and k < len(xqsfrq):
                               if xqksrq[k] != "" and xqsfrq[k] != "":
                                   insert_bgrspb_xx = "insert into " + bgrspb_xx_table_name + "(bgrguid,bgrxm,bgrxb,bgrcsrq,bgrmz,bgrjycd,bgrgz," \
                                                      "bgrhjsheng,bgrhjshi,bgrhjxian,bgrjzdsheng,bgrjzdshi,bgrjzdxian,bgrjzdxj," \
                                                      "dbrq,dbjg,fltw,zm,xflb,xq,hx,xqksrq,xqsfrq,bznx,fj,zw,wsmd5,ajzh,qksl," \
                                                      "cszh,bgrsl,spjg,ygr,jcy,bhur,qsjg,qszh,fzsj,fzddsheng,fzddshi,fzddxian," \
                                                      "saje,spz,spy,psy,sjy,pjrq,spcx,sfzs,wsddsheng,wsddshi,wsddxian,wslx,fzss,liyou,pjjg," \
                                                      "wsmc,shoubu,qkzm_id,qkpjrq,qksfrq,qkbz,qkxq,qkfj) " \
                                                      "values(%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                                                      "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                                                      "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                                                      "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" \
                                                      % (guid, xm_bg[i], str(xb_bg[i]), csrq_bg[i],getids(mz_bg[i], dic_mz),str(getids(jycd_bg[i], dic_jycd)),
                                                         getids(gz_bg[i], dic_gz), bgrhjsheng, bgrhjshi, bgrhjxian,bgrjzdsheng, bgrjzdshi, bgrjzdxian, bgrjzdxj,
                                                         dbrq_bg[i],dbjg_bg[i], fltw, zmid_str, xflb[i], xq[i], hx[i],xqksrq[k],xqsfrq[k], bznx[i], str(fj[i]),
                                                         temp_zw, ws_json[0],spzh,str(qksl), ajzh, str(bgrsl), spjg, ygr, jcy, bhur, qsjg,qszh,fzsj, fzddsheng,
                                                         fzddshi, fzddxian, saje, spz, spy, psy,sjy,pjrq, spcx, repr(sfzs), wsddsheng, wsddshi, wsddxian, wslx,
                                                         fzss, liyou, ws_json[5], ws_json[2], shoubu, qkzm_id,qkpjrq, qksfrq, qkbz,qkxq, str(qkfj))
                               else:
                                   # 这个insert少了刑期开始日期，刑期释放日期
                                   insert_bgrspb_xx = "insert into " + bgrspb_xx_table_name + "(bgrguid,bgrxm,bgrxb,bgrcsrq,bgrmz,bgrjycd,bgrgz," \
                                                      "bgrhjsheng,bgrhjshi,bgrhjxian,bgrjzdsheng,bgrjzdshi,bgrjzdxian,bgrjzdxj,dbrq,dbjg,fltw,zm," \
                                                      "xflb,xq,hx,bznx,fj,zw,wsmd5,ajzh,qksl,cszh,bgrsl,spjg,ygr,jcy,bhur,qsjg,qszh,fzsj,fzddsheng," \
                                                      "fzddshi,fzddxian,saje,spz,spy,psy,sjy,pjrq,spcx,sfzs,wsddsheng,wsddshi,wsddxian,wslx,fzss,liyou," \
                                                      "pjjg,wsmc,shoubu,qkzm_id,qkpjrq,qksfrq,qkbz,qkxq,qkfj) " \
                                                      "values(%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                                                      "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                                                      "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                                                      "'%s','%s','%s','%s','%s','%s')" \
                                                      % (guid, xm_bg[i], str(xb_bg[i]), csrq_bg[i],getids(mz_bg[i], dic_mz),str(getids(jycd_bg[i], dic_jycd)),
                                                         getids(gz_bg[i], dic_gz), bgrhjsheng, bgrhjshi, bgrhjxian, bgrjzdsheng, bgrjzdshi, bgrjzdxian, bgrjzdxj,
                                                         dbrq_bg[i],dbjg_bg[i], fltw, zmid_str, xflb[i], xq[i], hx[i], bznx[i], str(fj[i]), temp_zw, ws_json[0],
                                                         spzh, str(qksl), ajzh,str(bgrsl), spjg, ygr, jcy, bhur, qsjg, qszh, fzsj, fzddsheng,fzddshi, fzddxian,
                                                         saje, spz, spy, psy, sjy, pjrq, spcx,repr(sfzs), wsddsheng, wsddshi, wsddxian, wslx, fzss,liyou,ws_json[5],
                                                         ws_json[2], shoubu, qkzm_id, qkpjrq, qksfrq,qkbz, qkxq,str(qkfj))
                               k += 1
                           else:
                               insert_bgrspb_xx = "insert into " + bgrspb_xx_table_name + "(bgrguid,bgrxm,bgrxb,bgrcsrq,bgrmz,bgrjycd,bgrgz," \
                                                  "bgrhjsheng,bgrhjshi,bgrhjxian,bgrjzdsheng,bgrjzdshi,bgrjzdxian,bgrjzdxj,dbrq,dbjg,fltw,zm," \
                                                  "xflb,xq,hx,bznx,fj,zw,wsmd5,ajzh,qksl,cszh,bgrsl,spjg,ygr,jcy,bhur,qsjg,qszh,fzsj,fzddsheng," \
                                                  "fzddshi,fzddxian,saje,spz,spy,psy,sjy,pjrq,spcx,sfzs,wsddsheng,wsddshi,wsddxian,wslx,fzss,liyou," \
                                                  "pjjg,wsmc,shoubu,qkzm_id, qkpjrq, qksfrq, qkbz, qkxq, qkfj) " \
                                                  "values(%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                                                  "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                                                  "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                                                  "'%s','%s','%s','%s','%s','%s')" \
                                                  % (guid, xm_bg[i],str(xb_bg[i]),csrq_bg[i],getids(mz_bg[i],dic_mz),str(getids(jycd_bg[i],dic_jycd)),
                                                     getids(gz_bg[i],dic_gz),bgrhjsheng, bgrhjshi, bgrhjxian, bgrjzdsheng, bgrjzdshi,bgrjzdxian,bgrjzdxj,
                                                     dbrq_bg[i], dbjg_bg[i], fltw, zmid_str, xflb[i],xq[i],hx[i], bznx[i], str(fj[i]), temp_zw, ws_json[0],
                                                     spzh,str(qksl),ajzh, str(bgrsl), spjg, ygr, jcy, bhur, qsjg, qszh, fzsj,fzddsheng,fzddshi, fzddxian,
                                                     saje,spz,spy,psy,sjy,pjrq,spcx,repr(sfzs),wsddsheng,wsddshi,wsddxian,wslx,fzss,liyou,ws_json[5],
                                                     ws_json[2],shoubu,qkzm_id,qkpjrq,qksfrq,qkbz,qkxq,str(qkfj))
                           try:
                               cur.execute(insert_bgrspb_xx)
                           except Exception as e:
                               print(e)
                           j = 0  # 第三层循环，以这个人的信息inf_bg[i]这一段作为输入，拿到前科罪名，前科判决信息，前科刑期，前科剥政年限，前科罚金
                           while j < qksl:
                               qkzm_id, qkpjrq, qksfrq, qkbz, qkxq, qkfj = get_qkxx(qk_sentence[j],dic_zm)
                               try:
                                   cur.execute("insert into "+qkxx_table_name+"(qkzm,qkpjrq,qkxq,qkbznx,qkfj,qksfrq,bgrguid,ajzh,wsmd5,qklx)"
                                               "values('%s','%s','%s','%s','%s','%s',%s,'%s','%s','%s')"
                                               % (qkzm_id,qkpjrq,qkxq,qkbz,str(qkfj),qksfrq,guid,spzh,ws_json[0],str(1)))
                                   print("插入前科成功")
                               except Exception as e:
                                   print(e)
                               j += 1
                           index += 1
                           print(index)
               normal_insert()
           conn.commit()

index=0
xingchu_ah_list=[]
round_idlist=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19]
threads=[]
files=range(len(round_idlist))
conn = psycopg2.connect(database='justice', user='beaver', password='123456', host='58.56.137.206', port='5432')  # 127.0.0.1
cur = conn.cursor()
print("链接数据库成功")
dic_gz, dic_jycd, dic_mz, dic_spcx, dic_xb, dic_xflb, dic_zm, dic_sheng, dic_shi, dic_xian, dic_wslx = getdics(cur)
r = redis.Redis(host='58.56.137.206', port=6379,password="Lanhai@123")
for i in files:
    t=threading.Thread(target=inset_lianhe(round_idlist[i],'bgrspb_xx_test3','qkxx_test3'))
    threads.append(t)
if __name__=="__main__":
    for i in files:
        threads[i].start()
    for i in files:
        threads[i].join()
    print("ALL done!")
    conn.close()