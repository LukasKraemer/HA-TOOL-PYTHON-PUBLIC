# -*- coding: utf-8 -*-
# HA - Tool version 2.0b
#geschrieben von Lukas Krämer
# MIT LIZENZ
# 2020

from tkinter import Label, LabelFrame, Button, StringVar, OptionMenu, Entry, Tk, PhotoImage
from tkinter import ttk
import time 
import pandas as pd 
import sqlalchemy 
import pymysql
import fnmatch
import sys 
import shutil
import ctypes 
import threading 
from sys import platform 
import os, subprocess 
from datetime import datetime
import configparser
from PIL import ImageTk, Image
import webbrowser
from decimal import Decimal, localcontext
import getpass
import requests
import csv
import re



#Globale Variablen
config = configparser.ConfigParser() #config tool
now = datetime.now() # aktuelle uhrzeit
fred = threading.Thread() #prozessverwaltung
sperre = threading.Lock() # prozesssperre
login_db = str() #überprüfung ob Nutzer an der Datenbank angemeldet ist
engine= None #Datenbankverbindung
config.read("./config.ini") # Config  File

#Tabellennamen und Pfad
raw_data_tabelle = config.get("table", "raw_data_tabelle")
sprit_tabelle = config.get("table", "sprit_tabelle")
uebersichts_tabelle = config.get("table", "uebersichts_tabelle")
path = config.get("system", "path")
mode = config.get("system", "mode")
serverip = config.get("system", "serverip")
token = config.get("system", "token")
loggertable = config.get("table", "list_created_trips")

DB_IP_CONFIG= config.get("database", "database_ip")
DB_USER_CONFIG= config.get("database", "database_user")
DB_PASSWD_CONFIG= config.get("database", "database_passwd")
DB_PORT_CONFIG= config.get("database", "database_port")
DB_SCHEMA_CONFIG= config.get("database", "database_schema")
theads = config.get("database", "threads")
todo_trips= list()
fehlerliste = list()
cli = False
compact = True
if int(config.get("user", "farbmodi")) == 1:
    Farbmodus ='Farbmodus1'
else:
    Farbmodus ='Farbmodus2'

if int(config.get("user", "schriftfarbe")) == 1:
    schriftfarbe= "schriftfarbe1"
else:
    schriftfarbe = "schriftfarbe2"    

rot_schrift = int(config.get(schriftfarbe, "rot"))
gruen_schrift = int(config.get(schriftfarbe, "gruen"))
blau_schrift = int(config.get(schriftfarbe, "blau"))

rot = int(config.get(Farbmodus, "rot"))
gruen = int(config.get(Farbmodus, "gruen"))
blau = int(config.get(Farbmodus, "blau"))

farbcode_hintergrund= (rot, gruen, blau)


schriftfarberaw= (rot_schrift, gruen_schrift, blau_schrift)
restzeit= "Unbekannt"



try:
   if sys.argv[1]=="-nogui":
       cli = True
except:
    cli = False

try:
   if sys.argv[2]=="-compact":
       compact = True
except:
    compact = False

def is_admin():
    '''überprüft ob man in Windows als admin angemeldet ist'''
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False

def prompt_sudo():
    '''überprüft ob man in Linux admin ist'''
    ret = 0
    if os.geteuid() != 0:
        print("Bitte bitte mit root-Rechten anmelden!")
        msg = "[sudo] password for %u:"
        ret = subprocess.check_call("sudo -v -p '%s'" % msg, shell=True)
    return ret

def plattform_check():
    '''überprüft welches Betriebsystem installeirt ist und ob man Admin Rechte hat'''
    global path
    if platform == "linux" or platform == "linux2":
        if path == "nicht gesetzt":
            path= "/var/ha-tools/"
        #if prompt_sudo() != 0:
        #    print("kein admin")()
        #    sys.exit()
        

    elif platform == "win32":
        if path == "nicht gesetzt":
            path= "C:/ha-tools/"
        #ADMINRECHTE WERDEN NCIHT BENÖTIGT, falls doch bitte ausklammern
        #if is_admin():
        #    print("admin")
        #else:
        #    ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, __file__, None, 1)
        #    sys.exit()

    else:
        print ("unbekanntes System")
        time.sleep(5)
        sys.exit()
   
    if not(os.path.isdir(path)):
        if not compact:
            print("kein Ordner vorhanden - Ordner wird erstellt")
            os.makedirs(path)


def _from_rgb(rgb):
    """translates an rgb tuple of int to a tkinter friendly color code
    """
    return "#%02x%02x%02x" % rgb 

def open_github(event):
    try:
        webbrowser.open("https://github.com/LukasKraemer/HA_Tool")
    except:
        print("fehler beim Seite öffnen")


 
def send_data_to_server(file):
    global token
    global path
    multipart_form_data= {}
    multipart_form_data['APP'] = "tool"
    multipart_form_data['token'] = str(token)

    with open(path+file, "rb") as a_file:

        file_dict = {"uploadedfile": a_file}

        requests.post(serverip,data=multipart_form_data,  files=file_dict)
def login_value(User="", Pass="", Anschluss="", DB_schema="", ip="", create="No"):
    '''Verbindung zur Datenbank wird hergestellt und ein kleiner log eintrag auf die DB gemacht'''
    global login_db
    global engine
    global now

    if create != "No":
        if User != "":
            Nutzername= (User)
        else:
            Nutzername= DB_USER_CONFIG
        if Pass != "":  
            Passwort = (Pass)
        else:
            Passwort= DB_PASSWD_CONFIG
            
        if Anschluss != "":
            Port= str(Anschluss)
        else:
            Port= DB_PORT_CONFIG
        
        if DB_schema != "":
            Schema= (DB_schema)
        else:
            Schema = DB_SCHEMA_CONFIG

        if ip != "":
            adresse = ip
        else:
            adresse= DB_IP_CONFIG

        SQLALCHEMY_DATABASE_URI = f'mysql+pymysql://{Nutzername}:{Passwort}@{adresse}:{Port}/{Schema}'# [treiber]://[benutzername][passwort]@[IP]/[Schema in DB]
        engine = sqlalchemy.create_engine(SQLALCHEMY_DATABASE_URI)#herstellen der DB Verbindung
    else:
        pass
 
    try: 
        data = {'nutzername': [Nutzername], "Zeit" : [now.strftime("%d/%m/%Y, %H:%M:%S")], "Remote" : ip, "OS": [platform]} 
        logger = pd.DataFrame(data)
        logger.to_sql("loger", con=engine, if_exists='append')
        login_db= True
    except Exception as e:
        print (e)
        login_db=False
    if cli==False:       
        if login_db == True :
                loginsuccess.set("ANGEMELDET")
                label_loginsuccess.config(bg='green')
                root.update()
                return engine
        else:
            loginsuccess.set("Fehler!")
            root.update()  
    else:
        print("logged in")           
    

def lasttrip(tabellenname, tripid= "trip_counter"):
    '''ermittelt die letzte Fahrt auf der DB'''
    try:
        return pd.read_sql_query(f'SELECT {tripid} FROM {tabellenname} ORDER BY {tripid} DESC limit 1;',con=engine)
    except:
        print(f'lasttrip_fehler \n {tabellenname} \n {tripid}')
        return 0
def tripermitteln():
        '''gibt des zu verarbeitenden Trip zurück - Übersicht '''
        start= int()
        try:
            counteru = lasttrip(uebersichts_tabelle,"trip_nummer")
            start = counteru.at[0,'trip_nummer']+1 #wert um 1 erhoehen

            counterc =lasttrip(raw_data_tabelle)
            ziel = counterc.at[0,'trip_counter']
            if ziel == start:
                print("alles hochgeladen")
                return -1
            else:
                return int(start)
        except:
            
            print("Fehler")
            return 0

def update_gesamtanzeige(value, action = "value"):
    global cli, progressBarges
    if cli == False:
        '''Update der Gesamtanzeige'''

        if action == "value":    
            progressBarges['value']= int(value)
            progressBarges.update()
        else:
            progressBarges['maximum']= int(value)
            progressBarges.update()   
    else:
        pass         

        
def update_prozessanzeige(value, action = "value"):
    if cli == False:
        '''Update des einzelnen Prozess'''
        if action == "value":
            progressBarpro['value']=int(value)
            progressBarpro.update()
        else:
            progressBarpro['maximum']=int(value)
            progressBarpro.update()

def trip_handler(prozessanzahl):
    '''Verwaltet die Trips, jeder Prozess bekommt ein trip- callback fehlt'''
    global todo_trips
    
    wert= tripermitteln()

    for i in range(prozessanzahl):
        wert= wert+1
        todo_trips.append(wert)
    z=0
    while True:
        i=0

        for i in range(prozessanzahl):
            
            if todo_trips[i]=="weiter":
                wert= wert+1
                todo_trips[i]= wert

        for y in range(prozessanzahl):
            if todo_trips[i]=='fertig':
                z+=1
        if z == prozessanzahl:
            print("\nAlle berechnet")  
            sys.exit()
  

def trips(move=False):
    '''lädt die txt dateien auf die DB'''
    global path
    global engine
    time_ges=0
    
    fertig = int()
    regex = re.compile("Trip_20[1-3][0-9]-[0-2][0-9]-[0-3][0-9]_[0-3][0-9]-[0-9][0-9]-[0-9][0-9].txt")
    if login_db == True:       
        menge_trips =0
        for file in os.listdir(path):
            if regex.match(file):
                menge_trips = int(menge_trips) +1
        update_gesamtanzeige(menge_trips, "max")
        start_gesamt = datetime.now()
        time_einzeln= -1

        
        for file in os.listdir(path):#jede Datei im Ordner anschauen   
            if not compact:
                sys.stdout.write("\rhöchster Wert = %i" % fertig)
                sys.stdout.flush()  
            if regex.match(file):#wenn es eine Trip Datei ist   
                wertedertxtDatei = pd.read_csv(path+file, sep='\t')#einlesen der Text datei
                zahl = wertedertxtDatei.shape[0]#Länge der Datei bestimmen - zeilenanzahl für Trip_counter
                dupli = False
                
                try:
                    try:
                        #verhindert das doppelte Hochladen von txt Dateien
                        triplist = pd.read_sql_query(f'SELECT * from {loggertable};',con=engine)
                        for c in range(int(triplist.shape[0])):
                            if triplist['filename'][c] == file:
                                databasecheck = pd.read_sql_query(f'SELECT odo FROM {raw_data_tabelle} where counter=0 group by trip_counter;',con=engine)
                                for val in databasecheck:
                                    if val ==triplist['filename'][c] == zahl['odo']:
                                        print("doppelte Datei gefunden")
                                        if not(os.path.isdir(path+'fehler/')):
                                            if not compact:
                                                print("kein Ordner vorhanden - Ordner wird erstellt")
                                                os.makedirs(path+'fehler/')
                                        shutil.move(path+file, path+'fehler/')
                                        dupli = True
                                        break 
                    except:
                        pass
                    
                    

                except Exception as e:
                    #kann beim ersten starten ausgelöst werden
                    print("Fehler bei der Diplikatsuche"+ file)
                    #continue

                if(dupli):
                    fertig = fertig+1
                    update_gesamtanzeige(fertig)
                    continue
           
                try:#normalbetrieb
                    counter = pd.read_sql_query(f'SELECT trip_counter FROM {raw_data_tabelle} ORDER BY {raw_data_tabelle}.trip_counter DESC limit 1;',con=engine)#letzten Trip-counter aus der DB holen
                    counter.at[0,'trip_counter'] = int(counter.at[0,'trip_counter']) +1 #wert um 1 erhöhen
                except Exception as e:
                    data = {'trip_counter':[1]} 
                    counter = pd.DataFrame(data) 

                if(zahl >= 10 and wertedertxtDatei['speed_obd'].max() >=10):
                    #wenn die Fahrt weniger als zirka 10 sekunden ging und man nciht schneller als 10km/h fuhr
                    pass

                DBcounter = counter
                DBcounterpotenz = counter

                while( DBcounter.size < zahl):
                    if zahl >= DBcounter.size *2:
                        DBcounterpotenz = DBcounterpotenz.append(DBcounterpotenz, ignore_index = True)
                        DBcounter = DBcounter.append(DBcounterpotenz, ignore_index = True)
                    else: 
                        if DBcounter.size + DBcounterpotenz.size < zahl:
                            DBcounter = DBcounter.append(DBcounterpotenz, ignore_index = True)
                            DBcounterpotenz= DBcounterpotenz.loc[0:DBcounterpotenz.size/2]
                        elif DBcounterpotenz.size ==2:
                            DBcounter = DBcounter.append(DBcounterpotenz.loc[0], ignore_index = True)   
                        else:
                            DBcounterpotenz= DBcounterpotenz.loc[0:DBcounterpotenz.size/2]
                del DBcounterpotenz, counter             
                    
                new = wertedertxtDatei.join(DBcounter)#tripcounter plus werte
                new.to_sql(raw_data_tabelle, con=engine, if_exists='append', index='counter')
                if mode == "client":
                        send_data_to_server(file)  
                    
                if (move == True):
                    if not(os.path.isdir(path+"Archiv/")):
                            if not compact:
                                print("kein Ordner vorhanden - Ordner wird erstellt")
                                os.makedirs(path+"Archiv/")      
                    shutil.move(path+file, path+'Archiv/')#verschiebt die bearbeitete Datei ins archiv
                    #print(f'verschoben {fertig}')
                #Werte zurücksetzen
                del DBcounter
                fertig = fertig+1
                update_gesamtanzeige(fertig)
                now1 = datetime.now()
                time_einzeln = now1 - start_gesamt 
                trip_loggend = {'filename':[str(file)],
                                'Datum': [now1.strftime("%d/%m/%Y, %H:%M:%S")]                
                } 
                triplist = pd.DataFrame(trip_loggend)
                triplist.to_sql(loggertable, con=engine, if_exists='append') 
              
                
                if cli == False:
                    if time_einzeln == -1:
                        time_ges= time_einzeln.microseconds /1000 * menge_trips *1.5                   
                    restzeit= int((time_einzeln.seconds / fertig) * (menge_trips - fertig))
                    restzeitanzeige.set('Restzeit= ' + str(restzeit)+ "sek")
                
                    #restzeit= str(time_ges - time_einzeln * menge_trips-fertig /1000 *1.5) +" sekunden"
                    root.update()
                    ende_gesamt = datetime.now()
                    gesamt_zeit= ende_gesamt- start_gesamt   
                    print(f'Es dauerte {time_ges} Sekunden')        
                    update_gesamtanzeige(0)
                    update_prozessanzeige(0)
    else:
        print("nicht angemeldet")
    if not compact:
        now2= datetime.now()
        now2.strftime("%d/%m/%Y, %H:%M:%S")
        print("Ende Uploader:" +str(now2))

    if cli == False:
        if fertig!= 0:
            print(f'Fertig mit dem Upload \nEs Dauerte: {gesamt_zeit.seconds} Sekunden ')
      
def dataframe_difference(df1, df2, which=None):
        """Find rows which are different between two DataFrames."""
        comparison_df = df1.merge(df2,
                                  indicator=True,
                                  how='outer')
        if which is None:
            diff_df = comparison_df[comparison_df['_merge'] != 'both']
        else:
            diff_df = comparison_df[comparison_df['_merge'] == which]
        #diff_df.to_csv('data/diff.csv')
        return diff_df

def sprit(sicherheitsabfrage ="Y"):#parameter sprit
        """Läd die Tankdaten ein, Datei mit dem Namen 1012177_fuelings.csv sollte im Root liegen"""
        global uebersichts_tabelle
        try:
            df1 =pd.read_csv("1012177_fuelings.csv", sep=';')#einlesen von der CSV datei
            try:#Verssucht Werte aus der DB zu bekommen und zu vergleichen
                df2 = pd.read_sql_table('spritkosten',
                                con=engine,
                                index_col='id')
                diff_df = dataframe_difference(df2, df1)
            
            except:#sollte z.b. keine Werte vorhanden sein oder es andere Fehler geben, Nutzeraufforderung
                sicherheitsabfrage = input("FehLER beim lesen, weitermachen ohne vorheriges Einladen Y/n")
                if sicherheitsabfrage == "Y":
                    print("Fehler gefunden")
                    diff_df=df1
            
                #wenn Nutzer nciht zustimmt - Abbruch    
                else:
                    sys.exit()
        
            #Upload uf die DB 
            diff_df.to_sql(uebersichts_tabelle,
                con=engine,
                index=True,
                index_label='id',
                if_exists='append')
            print("fertig")
        except:
            print ("Keine Datei gefunden")

def uebersicht(theard_nr):
    
    '''generiert eine Übersicht, tripsweise'''
    global todo_trips
    global raw_data_tabelle
    global engine
    try:
        if not compact:
            print(todo_trips)
    
        if todo_trips[theard_nr] == "fertig":
            sys.exit
        if todo_trips[theard_nr] == "weiter":
            time.sleep(3)

        tripnummer= todo_trips[theard_nr]
    
        
            
        #progressBarbuilder_uebersicht(theard_nr)
        query = f"""
        SELECT * FROM {raw_data_tabelle}
        WHERE trip_counter = {tripnummer} ORDER BY Date asc; """
        trip_auswertung3_0 = pd.read_sql_query(query, engine)
        zeilenanzahl = trip_auswertung3_0.shape[0]
    
        if(zeilenanzahl <= 20):
            todo_trips[theard_nr]= "weiter"
            #print(f'keine Werte/ zu wenig gefunden für Trip {tripnummer}')
            if (zeilenanzahl == 0):
                todo_trips[theard_nr]= "fertig"
                sys.exit()
            else:
                time.sleep(0.3)
                uebersicht(theard_nr)    
        df4 = pd.DataFrame(columns=['soc'])
    
        x = 0        
        for x in range(0, zeilenanzahl): #alle 0er aus dem Datensatz hauen, die Akkuzelle kann nie 0 % haben
            if trip_auswertung3_0.at[x,'soc'] != 0:
                df4 = df4.append( {'soc': float(trip_auswertung3_0.at[x,'soc'])}, ignore_index=True)
        lastrow = int(zeilenanzahl-1)

        C_soc_durchschnittlich = trip_auswertung3_0['soc'].mean()

        C_soc_start = df4.at[0, "soc"]

        C_soc_min =df4['soc'].min()

        C_soc_max = trip_auswertung3_0['soc'].max()

        C_soc_ende = trip_auswertung3_0['soc'][zeilenanzahl-1]


        verbauch_durchschnitt= float(trip_auswertung3_0['tripfuel'][lastrow])/10/float(trip_auswertung3_0['trip_dist'][lastrow])#verbrauch km/l
        ev_anteil = float(trip_auswertung3_0['trip_dist'][lastrow])+ float(trip_auswertung3_0['trip_ev_dist'][lastrow])/2# Anteil der elektrisch gefahren wurde


        #der eigentliche Datensatz    
        uebersichtswerte ={'trip_nummer' : trip_auswertung3_0['trip_counter'][1],
                            'tag': pd.Timestamp(trip_auswertung3_0['Date'][0]),
                            'uhrzeit_Beginns' : trip_auswertung3_0['Time'][0], 
                            'uhrzeit_Ende': trip_auswertung3_0['Time'][lastrow],
                            'kmstand_start' : trip_auswertung3_0['odo'][0],
                            'trip_laenge': trip_auswertung3_0['trip_dist'][lastrow],
                            'trip_laengeev': trip_auswertung3_0['trip_ev_dist'][lastrow],
                            'fahrzeit': trip_auswertung3_0['trip_nbs'][lastrow],
                            'fahrzeit_ev': [trip_auswertung3_0['trip_ev_nbs'][lastrow]],
                            'fahrzeit_bewegung':  trip_auswertung3_0['trip_mov_nbs'][lastrow],
                            'spritverbrauch': [trip_auswertung3_0['tripfuel'][lastrow]],
                            'max_aussentemperatur':[trip_auswertung3_0['ambient_temp'].max()], 
                            'aussentemperatur_durchschnitt' : [trip_auswertung3_0['ambient_temp'].mean()],
                            'soc_durchschnitt' : [C_soc_durchschnittlich],
                            'soc_minimum':[C_soc_min],
                            'soc_maximal':[C_soc_max],
                            'soc_start':[C_soc_start],
                            'soc_ende':[C_soc_ende],
                            'verbauch_durchschnitt': [float(verbauch_durchschnitt)],
                            'ev_anteil':[int(ev_anteil)],
                            'geschwindichkeit_durchschnitt':[trip_auswertung3_0['speed_obd'].mean()],
                            'geschwindichkeit_maximal':[trip_auswertung3_0['speed_obd'].max()],
                            'soc_veraenderung': [int(C_soc_start) - int(C_soc_ende)  ]
                            
                            } 
    

            
        uebersichtges = pd.DataFrame(data=uebersichtswerte)
        
        del trip_auswertung3_0
        del uebersichtswerte
        
        sperre.acquire()
        uebersichtges.to_sql(uebersichts_tabelle,
                    con=engine,
                    index=True,
                    index_label='id',
                    if_exists='append')
        sperre.release()  
        del uebersichtges
        if not compact:
            print ("fertig"+ str(tripnummer))
        todo_trips[theard_nr]="weiter"
        time.sleep(0.1)    
        uebersicht(theard_nr=theard_nr) 
   
   
    except ZeroDivisionError:
        fehlerliste.append(tripnummer)
        todo_trips[theard_nr]="weiter"
        time.sleep(0.1)    
        uebersicht(theard_nr=theard_nr)      
    if not compact:
        print("Uebersicht Fertig")

def Programmauswahl(programm, prozess=1):
    '''startet die Programme und übergibt ihnen gegebenfalls die Werte'''
    prozesse = []
    if programm =="trips":
        p1= threading.Thread(target=trips, args=(True,))
        p1.start()
    elif programm == "sprit":
        p2= threading.Thread(target=sprit, args=(False,))
        p2.start()
    elif programm == "ueberischt":
        diff =  lasttrip(raw_data_tabelle)['trip_counter'][0] - lasttrip(uebersichts_tabelle,"trip_nummer")['trip_nummer'][0]
        if diff == 0:
            print("keine neuen Werte")
            sys.exit()
        elif diff < int(prozess):
            print("weniger als prozesse")
            theadcount = int(diff)
        else:
            theadcount = int(prozess)        
        p3= threading.Thread(target=trip_handler, args=(theadcount,))
        p3.start()
        time.sleep(10)
        i=1
        for i in range (int(theadcount)):
            threading.Thread()
            prozesse.append(threading.Thread(target=uebersicht, args=(i,)))
            prozesse[i].start()

    else:
        print("unbekanntes Programm")       
        
plattform_check()# überprüfung des betriebsystem



    
if cli ==False:
    try:
        #Grafische Anzeige
        root = Tk()


        #Hintergrundfarbe für alle Elemtente
        hintergrundfarbe= _from_rgb(farbcode_hintergrund)
        schriftfarbe = _from_rgb(schriftfarberaw)

        root.title('HA- Tool')
        root.iconphoto(True, PhotoImage(file="Logo.png"))
        root.geometry("420x350")
        root.configure(bg=hintergrundfarbe) 

        gesFrame = LabelFrame()
        gesFrame.grid(column=0,row=1)
        gesFrame.configure(bg=hintergrundfarbe, foreground=schriftfarbe) 

        #grafikoberfläche
        buttonFrame = LabelFrame(gesFrame,text="Programmauswahl")
        buttonFrame.grid(column=0,row=1)
        buttonFrame.configure(bg=hintergrundfarbe, foreground=schriftfarbe) 


        #Knopf der Alle Trips einläd
        button1 = ttk.Button(buttonFrame, text="Trips" ,command= lambda pragramm="trips" :Programmauswahl(pragramm))
        button1.grid(column = 0, row = 2,  padx=5, pady=5)

        #Läd die Sprittabelle ein
        button2 = ttk.Button(buttonFrame, text="Sprit " ,command= lambda pragramm="sprit" :Programmauswahl(pragramm))
        button2.grid(column = 1, row = 2, padx=5, pady=5)


        #Prozess auswahl
        prozess = StringVar(root)

        choices = { 1,2,3,4,6,8,12,16}
        prozess.set(theads) # set the default option

        prozessanzahl = OptionMenu(buttonFrame, prozess, *choices)
        prozessanzahl.grid(row = 2, column =4)

        #Übersicht wird erstellt - CPU Anzahl bearbeiten und Multicore
        button3 = ttk.Button( buttonFrame, text="Übersicht ", command=lambda pragramm="ueberischt" :Programmauswahl(pragramm, int(prozess.get())))
        button3.grid(column = 3, row = 2, padx=5, pady=5)



        progesFrame = LabelFrame(gesFrame,text="Gesamtfortschritt")
        progesFrame.grid(column=0,row=3)
        progesFrame.configure(bg=hintergrundfarbe, foreground=schriftfarbe) 

        #Prozessor die den gesammten Fortschritt anzeigen soll
        progressBarges = ttk.Progressbar(progesFrame, orient="horizontal", length=400)
        progressBarges.grid(column = 1, row = 4, columnspan=4, padx=5, pady=5)
        progressBarges['value']=0

        #Prozess 1- zeigt den ungefähren Forschritt an beim erstellen der Ueberischt
        progressBarpro = ttk.Progressbar(progesFrame, orient="horizontal", length=400)
        progressBarpro.grid(column = 1, row = 5, padx=5, pady=5)
        progressBarpro['value']=0


        loginFrame = LabelFrame(gesFrame,text="Login für DB")
        loginFrame.grid(column=0,row=6)
        loginFrame.configure(bg=hintergrundfarbe, foreground=schriftfarbe)



        image = Image.open("Logo.png")
        image = image.resize((100,100), Image.ANTIALIAS)
        photoImg =  ImageTk.PhotoImage(image)

        label = Label(loginFrame,image=photoImg)
        label.image = photoImg # this line need to prevent gc
        label.bind( "<Button>", open_github )
        label.grid(row=7, column=5, padx=5, pady=5, rowspan=5)


        restzeitanzeige = StringVar()


        restzeitanzeige.set('Restzeit= ' +restzeit)

        #Beschriftungen
        l1= Label(loginFrame, text="Benutzername*")
        l2= Label(loginFrame, text="Passwort*")
        l3= Label(loginFrame, text="Schema")
        l4= Label(loginFrame, text="Port")
        l5= Label(loginFrame, text="IP Adresse")
        l6= Label(loginFrame, text="Restzeit", textvariable=restzeitanzeige)

        l1.configure(bg=hintergrundfarbe, foreground=schriftfarbe)
        l2.configure(bg=hintergrundfarbe, foreground=schriftfarbe)
        l3.configure(bg=hintergrundfarbe, foreground=schriftfarbe)
        l4.configure(bg=hintergrundfarbe, foreground=schriftfarbe)
        l5.configure(bg=hintergrundfarbe, foreground=schriftfarbe)
        l6.configure(bg=hintergrundfarbe, foreground=schriftfarbe)

        l1.grid(row=7)
        l2.grid(row=8)
        l3.grid(row=9)
        l4.grid(row=10)
        l5.grid(row=11)
        l6.grid(row=13, column=5)


        #logindaten
        User = StringVar()
        Pass = StringVar()
        DB_schema = StringVar()
        anschluss = StringVar()
        ip = StringVar()
        loginsuccess = StringVar()


        loginsuccess.set("NICHT ANGEMELDET")
        User.set(DB_USER_CONFIG)
        Pass.set(DB_PASSWD_CONFIG)
        ip.set(DB_IP_CONFIG)
        anschluss.set(DB_PORT_CONFIG)
        DB_schema.set(DB_SCHEMA_CONFIG)

        #Eingaben
        e1 = Entry(loginFrame,textvariable=User)
        e2 = Entry(loginFrame, show="*", textvariable=Pass)
        e3 = Entry(loginFrame, textvariable=DB_schema)
        e4 = Entry(loginFrame, textvariable=anschluss)
        e5 = Entry(loginFrame, textvariable=ip)
        e6 = Entry(loginFrame, textvariable=restzeit)


        button4 = ttk.Button( loginFrame, text="LOGIN",command= lambda: login_value(str(User.get()),str(Pass.get()), str(anschluss.get()),str(DB_schema.get()),str(ip.get()),create="yes"))
        button4.grid(column = 1, row = 12, padx=5, pady=5,)

        label_loginsuccess= Label(loginFrame, bg = "red",textvariable=loginsuccess)
        label_loginsuccess.grid(row=13)
        e1.grid(row=7, column=1)
        e2.grid(row=8, column=1)
        e3.grid(row=9, column=1)
        e4.grid(row=10, column=1)
        root.mainloop()
    except:
        print("catch")
else:
    #print("start upload")
    login_value(create="yes")
    Programmauswahl(programm="trips")
    time.sleep(10)
    
    print("start überischt")
    Programmauswahl("ueberischt", prozess=theads) 
