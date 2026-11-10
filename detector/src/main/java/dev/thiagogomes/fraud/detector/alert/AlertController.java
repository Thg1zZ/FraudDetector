package dev.thiagogomes.fraud.detector.alert;

import java.util.List;
import java.util.UUID;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@RequestMapping("/alerts")
@Tag(name = "Alertas", description = "Consulta e classificação dos alertas detectados")
public class AlertController {
    private final AlertRepository repository;

    public AlertController(AlertRepository repository) { this.repository = repository; }

    @GetMapping
    @Operation(summary = "Listar alertas", description = "Filtros combinados; até 100 resultados por página")
    @Transactional(readOnly = true)
    public AlertPage list(@RequestParam(required = false) FraudRule rule,
                          @RequestParam(required = false) AlertStatus status,
                          @RequestParam(required = false) @Size(max = 64) String cardId,
                          @RequestParam(defaultValue = "0") @Min(0) @Max(10000) int page,
                          @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
        Specification<AlertEntity> filter = Specification.allOf();
        if (rule != null) filter = filter.and((root, query, cb) -> cb.equal(root.get("rule"), rule));
        if (status != null) filter = filter.and((root, query, cb) -> cb.equal(root.get("status"), status));
        if (cardId != null) filter = filter.and((root, query, cb) -> cb.equal(root.get("cardId"), cardId));
        var result = repository.findAll(filter, PageRequest.of(page, size, Sort.by(
                Sort.Order.desc("detectedAt"), Sort.Order.desc("id"))));
        return new AlertPage(result.map(AlertResponse::from).getContent(), page, size,
                result.getTotalElements(), result.getTotalPages());
    }

    @GetMapping("/{id}")
    @Operation(summary = "Consultar detalhes de um alerta")
    @Transactional(readOnly = true)
    public AlertResponse get(@PathVariable UUID id) { return AlertResponse.from(find(id)); }

    @PatchMapping("/{id}/status")
    @Operation(summary = "Classificar como fraude confirmada ou falso positivo")
    @Transactional
    public AlertResponse updateStatus(@PathVariable UUID id, @Valid @RequestBody StatusUpdate request) {
        AlertEntity alert = find(id);
        alert.status = AlertStatus.valueOf(request.status());
        return AlertResponse.from(alert);
    }

    private AlertEntity find(UUID id) {
        return repository.findById(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Alerta não encontrado"));
    }

    public record StatusUpdate(@NotNull @Pattern(regexp = "CONFIRMED|FALSE_POSITIVE") String status) { }
    public record AlertPage(List<AlertResponse> content, int page, int size, long totalElements, int totalPages) { }
}
