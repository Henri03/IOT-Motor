# IOT-Motor Raspberry Pi Projekt

Dieses Repository enthält das Code- und Konfigurationsmaterial für das IOT-Motor-Projekt, das auf einem Raspberry Pi läuft. Es beinhaltet Anweisungen zur Einrichtung, zum Starten der Anwendung und zur Fehlerbehebung.

---

## 1. SSH-Zugang zum Raspberry Pi

Um auf den Raspberry Pi zuzugreifen, können Sie die folgenden SSH-Konfigurationen verwenden. Es sind Konfigurationen für WLAN und LAN enthalten.

**SSH-Konfiguration (`~/.ssh/config`):**

Host PC3
#wifi
HostName 192.168.178.26
#lan
#HostName 192.168.0.30
User admin

**SSH-Anmeldedaten:**

*   **Benutzername:** `admin`
*   **Passwort:** `admin`

---

## 2. Projekt starten

Befolgen Sie diese Schritte, um die Anwendung auf dem Raspberry Pi zu starten.

### 2.1 Virtuelle Umgebung aktivieren

Stellen Sie sicher, dass die virtuelle Python-Umgebung aktiviert ist, bevor Sie weitere Befehle ausführen.


source venv/bin/activate
###2.2 Docker-Container bauen
Bauen Sie die Docker-Images für die Anwendung.


    
docker compose build

Fehlerbehebung beim Bauen:
Sollte es während des Bauprozesses zu Fehlern kommen, die mit Redis zusammenhängen, versuchen Sie, den Redis-Server zu stoppen:


    
sudo systemctl stop redis-server
---
###2.3 Docker-Container starten
Starten Sie die Docker-Container im Detached-Modus (im Hintergrund).


    
docker compose up -d

##3. Zugriffs-URLs
Nachdem die Docker-Container erfolgreich gestartet wurden, können Sie auf die folgenden Dienste zugreifen:

Portainer:

URL: http://192.168.178.26:9000
Anmeldedaten:
Benutzername: admin
Passwort: abcdefghijkl
Web-Anwendung (Django Admin):

URL: http://192.168.178.26:8000/admin/
Anmeldedaten (Django Admin):
Benutzername: henri
Passwort: admin
##4. Fehlerbehebung Webserver-Zugriff
Falls Sie die Web-Anwendung unter http://192.168.178.26:8000 nicht erreichen können:

DJANGO_ALLOWED_HOSTS überprüfen:
Überprüfen und passen Sie die Umgebungsvariable DJANGO_ALLOWED_HOSTS in Ihrer .env-Datei an, um sicherzustellen, dass die IP-Adresse des Raspberry Pi oder * (für alle Hosts, nur zu Entwicklungszwecken empfohlen) enthalten ist.

Docker Compose Logs einsehen:
Überprüfen Sie die Logs des Web-Containers, um Fehlermeldungen im Terminal zu sehen, die auf das Problem hinweisen könnten:


    
docker compose logs web
