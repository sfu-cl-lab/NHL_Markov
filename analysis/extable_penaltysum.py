import MySQLdb
def transtime(timestr):
	timestr=str(timestr)
	timestr=timestr.split(":")
	return int(timestr[1])*60+int(timestr[2])
hometeamid=0
awayteamid=0
timetick=-1
homeps=0
awayps=0
conn=MySQLdb.connect(host='localhost',
                    user='research',
                    passwd='asdfgh',
                    db='nhl_final')
cur = conn.cursor()
cur.execute ("SELECT DISTINCT `GameId` FROM `play-by-play`")
gameids = cur.fetchall()
for gameidr in gameids:
	homeps=0
	awayps=0
	for periodnumber in range(1,21):
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
				cur.execute("UPDATE `play-by-play` SET `HomePenaltySum`=%d, `AwayPenaltySum`=%d WHERE `GameId`=%d AND `PeriodNumber`=%d AND `EventNumber`=%d"%(homeps,awayps,int(gameidr[0]),periodnumber,int(recordr[0])))
				conn.commit() #set number information
				if recordr[2]=="PENALTY":
					try:
						cur.execute("SELECT `TeamPenaltyId`,`PenaltyDuration` FROM `penalty-info` WHERE `GameId`=%d AND `PeriodNumber`=%d AND `EventNumber`=%d LIMIT 1"%(int(gameidr[0]),periodnumber,int(recordr[0])))
						pt=cur.fetchone()
						ptd=int(pt[1])
						if ptd!=0:
							if int(pt[0])==hometeamid: #hometeam get penalty
								homeps=homeps+1	
							else:
								awayps=awayps+1
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
