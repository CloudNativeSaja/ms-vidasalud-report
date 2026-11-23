package cl.duoc.vidasalud.report.listener;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;

import cl.duoc.vidasalud.report.service.KpiService;
import jakarta.validation.Validation;

/**
 * Los KPIs viven solo en memoria: al reiniciar, el grupo ya tiene offsets
 * confirmados y, si no releyera el topico, los indicadores quedarian en cero.
 */
class ReplayOnAssignmentTest {

    @Test
    void alAsignarseParticionesRelaeDesdeElInicio() {
        AppointmentEventListener listener = new AppointmentEventListener(
                mock(KpiService.class), Validation.buildDefaultValidatorFactory().getValidator());
        ConsumerSeekCallback callback = mock(ConsumerSeekCallback.class);

        listener.onPartitionsAssigned(Map.of(
                new TopicPartition("vidasalud.appointments.events", 0), 7L,
                new TopicPartition("vidasalud.appointments.events", 2), 9L), callback);

        verify(callback).seekToBeginning("vidasalud.appointments.events", 0);
        verify(callback).seekToBeginning("vidasalud.appointments.events", 2);
        verifyNoMoreInteractions(callback);
    }
}
