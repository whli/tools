#coding=utf-8

def cal_num(value_list,max_value):
    '''
    计算列表在各四分位的频数统计
    in:(list,max(list))
    '''
    #[一分位数量，。。。，总数]
    result = [0]*5
    p1 = float(max_value)*0.25
    p2 = float(max_value)*0.5
    p3 = float(max_value)*0.75
    for value in value_list:
        result[4] += 1
        if value < p1:
            result[0] += 1
        elif value >= p1 and value < p2:
            reslut[1] += 1
        elif value >= p2 and value < p3:
            result[2] += 1
        elif value >= p3 and value <= max_value:
            reslut[3] += 1
        else:
            pass
    print "总doc数量：",result[4]
    print "[0-%s):" %(p1),result[0]," ;占比",float(result[0])/result[4]
    print "[%s-%s):" %(p1,p2),result[1]," ;占比",float(result[1])/result[4]
    print "[%s-%s):" %(p2,p3),result[2]," ;占比",float(result[2])/result[4]
    print "[%s-%s]:" %(p3,max_value),result[3]," ;占比",float(result[3])/result[4]

