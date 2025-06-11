import pyttsx3
import speech_recognition as sr#also download pip install pyaudio 
import datetime
import wikipedia
import webbrowser
import os
engin=pyttsx3.init("sapi5")#to get access with inbuild voice from microsoft(sapi5)
voices=engin.getProperty('voices')
print(voices[0].id)
engin.setProperty('voice',voices[1].id)

def speak(audio):#this function take all the statement from function call
    engin.say(audio)#engin allredy as property
    engin.runAndWait()
def wish():
    hour=int(datetime.datetime.now().hour)
    if(hour>=0 and hour<12):
        speak("good morning MR devesh")
    elif(hour>=12 and hour<18):
        speak("good afternoon MR devesh")
    else:
        speak("good evening MR devesh")
    speak("hope your day is going well today,how can i help you")

def takeCommand():
    r = sr.Recognizer()
    with sr.Microphone() as source:#take our cammad as an input
        print("Adjusting for ambient noise...")  # Optional
        r.adjust_for_ambient_noise(source, duration=1)  # Adjust for ambient noise
        print("Listening...")
        
        try:
            audio = r.listen(source, timeout=5, phrase_time_limit=10)#convert voice to string and strore it in audio
            print("Audio captured. Processing...")
        except sr.WaitTimeoutError:
            print("Listening timed out while waiting for phrase to start.")
            return "None"
    
    try:
        print("Recognizing...")
        query = r.recognize_google(audio, language='en-in')#audio is then passed to google to recognize which is in indian english
        print(f"user said: {query}\n")
    except sr.UnknownValueError:
        print("Google Speech Recognition could not understand audio")
        return "None"
    except sr.RequestError as e:
        print(f"Could not request results from Google Speech Recognition service; {e}")
        return "None"
    
    return query
if __name__ == "__main__":#it execute when we run the code
    wish()
    while 1:
        query=takeCommand().lower()
        if 'wikipedia' in query:
            speak("serching in wikipedia")
            query=query.replace("wikipedia","")
            result=wikipedia.summary(query,sentences=2)
            speak("according to wikipedia")
            print(result)
            speak(result)
            break
        elif "rashmika open youtube" in query:
            webbrowser.open("youtube.com")
            break
        elif "rashmika open google" in query:
            webbrowser.open("google.com")
            break
        elif "rashmika stackoverflow" in query:
            webbrowser.open("stackoverflow.com")
            break
        elif "rashmika what's the time" in query:
            strtime=datetime.datetime.now().strftime("%H:%M:%S")#convert to string
            print(strtime)
            speak(strtime)
            break
        elif "rashmika open path" in query:#same code can be written for many other also
            openpath="C:\\Users\\dheve\\Documents\\Microsoft VS Code\\Code.exe"
            os.startfile(openpath)
            break
        elif "what's your name" in query:
            speak("you can call me rashmika")
      

    