from django.core.management.base import BaseCommand
from iot_app.models import ReferenceRun
from statistics import mean

class Command(BaseCommand):
    help = "Berechne Mittelwerte aus validen Referenzläufen"

    def handle(self, *args, **kwargs):
        refs = ReferenceRun.objects.filter(is_valid=True)
        if refs.count() == 0:
            self.stdout.write("Keine Referenzläufe gefunden!")
            return
        avg_current = mean([r.current for r in refs])
        avg_voltage = mean([r.voltage for r in refs])
        avg_rpm = mean([r.rpm for r in refs])
        self.stdout.write(f"Erwartete Werte - Strom: {avg_current:.2f}, Spannung: {avg_voltage:.2f}, Drehzahl: {avg_rpm:.2f}")