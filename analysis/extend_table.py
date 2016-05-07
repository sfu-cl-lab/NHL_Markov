#Zeyu Zhao. Calculate people on ice when event happens
import MySQLdb
class penaltyqueue:
	__length=0
	__timeback=[]
	def __init__(self):
		self.__timeback=[0]*6
	def put(self,time):#expected time to return
		self.__length=self.__length+1
		for i in range(0,5):
			if self.__timeback[i]==0:
				break
		self.__timeback[i]=time
	def getnum(self,time):#now num of players in penalty box
		for i in range (0,5):
			if self.__timeback[i]!=0 and self.__timeback[i]<time:
				self.__timeback[i]=0
				self.__length=self.__length-1
		return self.__length
	def cancel(self,time):#cancel penalty ending at time
		for i in range (0,5):
			if self.__timeback[i]==time:
				self.__length=self.__length-1
				self.__timeback[i]=0
				break
	def cancelsmall(self):#cancelsmallest
		j=9999
		k=0
		if self.__length!=0:
			for i in range (0,5):
				if self.__timeback[i]!=0 and self.__timeback[i]<j:
					k=i
					j=self.__timeback[i]
			self.__timeback[k]=0
			self.__length=self.__length-1
	def empty(self):
		self.__length=0
		self.__timeback=[0]*6
	def stillin(self,time):
		g=[]
		if self.getnum(time)!=0:
			for i in range(0,5):
				if self.__timeback[i]!=0:
					g.append(self.__timeback[i]-time)
		return g
def transtime(timestr):
	timestr=str(timestr)
	timestr=timestr.split(":")
	return int(timestr[1])*60+int(timestr[2])
homepenalty=penaltyqueue()
awaypenalty=penaltyqueue()
hometeamid=0
awayteamid=0
timetick=-1
homepq=[]
awaypq=[]
homegoli=0
awaygoli=0
homegolid=-1
awaygolid=-1
conn=MySQLdb.connect(host='localhost',
                    user='research',
                    passwd='asdfgh',
                    db='nhl_final')
cur = conn.cursor()
cur.execute ("SELECT DISTINCT `GameId` FROM `play-by-play`")
gameids = cur.fetchall()
for gameidr in gameids:
	homepenalty.empty()
	awaypenalty.empty()
	for periodnumber in range(1,21):
		if str(gameidr[0])[4:5]=='02' and periodnumber>4:#for season 02, won't carry to 5+
			homepenalty.empty()
			awaypenalty.empty()
		hqsi=homepenalty.stillin(20*60)#set penalty that carry to next period
		aqsi=awaypenalty.stillin(20*60)
		homepenalty.empty()
		awaypenalty.empty()
		homegoli=0
		awaygoli=0
		if len(hqsi)!=0:
			for i in range(0,len(hqsi)):
				homepenalty.put(hqsi[i])
		if len(aqsi)!=0:
			for i in range(0,len(aqsi)):
				awaypenalty.put(aqsi[i])


		timetick=-1
		try:
			cur.execute("SELECT `AwayTeamId`,`HomeTeamId` FROM `play-by-play` WHERE `GameId`=%d AND `PeriodNumber`=%d AND `AwayTeamId` is not NULL AND `HomeTeamId` is not NULL LIMIT 1"%(int(gameidr[0]),periodnumber))
			r=cur.fetchone()
			if r==None:
				break
			hometeamid=int(r[1])
			awayteamid=int(r[0])
			cur.execute("SELECT `EventNumber`,`EventTime`,`EventType`,`ActionSequence` From `play-by-play` WHERE `GameId`=%d AND `PeriodNumber`=%d ORDER BY `EventNumber` ASC"%(int(gameidr[0]),periodnumber))
			records=cur.fetchall()
			for recordr in records:
				if recordr[3]==None or int(recordr[3])!=homegolid:
					homegoli=0
				if recordr[3]==None or int(recordr[3])!=awaygolid:
					awaygoli=0
				if transtime(recordr[1])!=timetick: #Not happen at the same time
					homepq=[]
					awaypq=[]
					timetick=transtime(recordr[1])
				cur.execute("UPDATE `play-by-play` SET `HomePlayerNumber`=%d, `AwayPlayerNumber`=%d WHERE `GameId`=%d AND `PeriodNumber`=%d AND `EventNumber`=%d"%(homegoli+5-homepenalty.getnum(timetick),awaygoli+5-awaypenalty.getnum(timetick),int(gameidr[0]),periodnumber,int(recordr[0])))
				conn.commit() #set number information
				if recordr[2]=="GOAL":
					try:
						cur.execute("SELECT `ScoringTeamId` From `event_goal` WHERE `GameId`=%d AND `PeriodNumber`=%d AND `EventNumber`=%d LIMIT 1"%(int(gameidr[0]),periodnumber,int(recordr[0])))
						goalt=cur.fetchone()
						if homepenalty.getnum(timetick)!=awaypenalty.getnum(timetick):
							if int(goalt[0])==awayteamid:
								homepenalty.cancelsmall()
							else:
								awaypenalty.cancelsmall()
					except Exception,e:
						print 'error: gameid='+str(gameidr[0])+", periodnumber="+str(periodnumber)
						print e
						pass
				elif recordr[2]=="PENALTY":
					try:
						cur.execute("SELECT `TeamPenaltyId`,`PenaltyDuration` FROM `penalty-info` WHERE `GameId`=%d AND `PeriodNumber`=%d AND `EventNumber`=%d LIMIT 1"%(int(gameidr[0]),periodnumber,int(recordr[0])))
						pt=cur.fetchone()
						ptd=int(pt[1])
						if ptd!=0:
							if int(pt[0])==hometeamid: #hometeam get penalty
								if ptd in awaypq: #cancel
									del awaypq[awaypq.index(ptd)]
									awaypenalty.cancel(timetick+ptd*60)
								else:
									homepq.append(ptd)
									homepenalty.put(timetick+ptd*60)
							else:
								if ptd in homepq:
									del homepq[homepq.index(ptd)]
									homepenalty.cancel(timetick+ptd*60)
								else:
									awaypq.append(ptd)
									awaypenalty.put(timetick+ptd*60)
					except Exception,e:
						print 'error: gameid='+str(gameidr[0])+", periodnumber="+str(periodnumber)
						print e
						pass
				elif recordr[2]=="GOALIE":#PULL GOALIE
					try:
						cur.execute("SELECT `TeamPullingGoalieId` FROM `event_goalie_pulled` WHERE `GameId`=%d AND `PeriodNumber`=%d AND `EventNumber`=%d LIMIT 1"%(int(gameidr[0]),periodnumber,int(recordr[0])))
						pt=cur.fetchone()
						tid=int(pt[0])
						if tid==hometeamid:
							homegoli=1
							if recordr[3]!=None:
								homegolid=int(recordr[3])
							else:
								homegolid=-1
						elif tid==awayteamid:
							awaygoli=1
							if recordr[3]!=None:
								awaygolid=int(recordr[3])
							else:
								awaygolid=-1

					except Exception,e:
						print 'error: gameid='+str(gameidr[0])+", periodnumber="+str(periodnumber)
						print e
						pass
		except Exception,e:
			 print 'error: gameid='+str(gameidr[0])+", periodnumber="+str(periodnumber)
			 print e
			 pass
#cur.execute("UPDATE `play-by-play` SET `HomePlayerNumber`=6, `AwayPlayerNumber`=6 WHERE `PeriodNumber`>5")
#cur.commit()
				
cur.close()
conn.close()
