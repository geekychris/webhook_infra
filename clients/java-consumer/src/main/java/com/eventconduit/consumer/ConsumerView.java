package com.eventconduit.consumer;

import com.eventconduit.consumer.StreamConsumerService.ConnectionState;
import com.vaadin.flow.component.*;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.combobox.ComboBox;
import com.vaadin.flow.component.details.Details;
import com.vaadin.flow.component.grid.Grid;
import com.vaadin.flow.component.html.*;
import com.vaadin.flow.component.notification.Notification;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.textfield.NumberField;
import com.vaadin.flow.component.textfield.TextField;
import com.vaadin.flow.router.PageTitle;
import com.vaadin.flow.router.Route;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Consumer;

@Route("")
@PageTitle("Event Conduit — Java Stream Consumer")
public class ConsumerView extends VerticalLayout {

    private final StreamConsumerService streamService;
    private final RegistryClient registry;
    private final UI ui;

    private final TextField gatewayField = new TextField("Gateway URL");
    private final ComboBox<RegistryClient.StreamTarget> subscriptionPicker = new ComboBox<>("Subscription");
    private final TextField tokenField = new TextField("Auth Token");
    private final NumberField resumeField = new NumberField("Resume After");
    private final Button connectBtn = new Button("Connect");
    private final Button disconnectBtn = new Button("Disconnect");
    private final Button clearBtn = new Button("Clear");
    private final Button refreshBtn = new Button("Refresh Subscriptions");

    private final Span statusBadge = new Span();
    private final Span statsLabel = new Span();
    private final Grid<ReceivedEvent> grid = new Grid<>();
    private final Pre detailPre = new Pre();

    private Consumer<ReceivedEvent> eventListener;
    private Consumer<ConnectionState> stateListener;

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter
        .ofPattern("HH:mm:ss.SSS")
        .withZone(ZoneId.systemDefault());

    public ConsumerView(StreamConsumerService streamService, RegistryClient registry) {
        this.streamService = streamService;
        this.registry = registry;
        this.ui = UI.getCurrent();

        setSizeFull();
        setPadding(true);
        setSpacing(true);
        getStyle().set("background", "#0f1923").set("color", "#e8edf2");

        add(buildHeader());
        add(buildConnectionPanel());
        add(buildStatusBar());
        add(buildEventGrid());
        add(buildDetailPanel());

        registerListeners();
        updateStatus();
        refreshGrid();
        loadSubscriptions();
    }

    private Component buildHeader() {
        var title = new H2("Event Conduit");
        title.getStyle().set("color", "#3b82f6").set("margin", "0");
        var subtitle = new Span("— Java Stream Consumer");
        subtitle.getStyle().set("color", "#8899aa").set("margin-left", "8px");
        var header = new HorizontalLayout(title, subtitle);
        header.setAlignItems(Alignment.BASELINE);
        return header;
    }

    private Component buildConnectionPanel() {
        gatewayField.setValue("ws://localhost:18090/v1/stream");
        gatewayField.setWidth("300px");

        subscriptionPicker.setWidth("400px");
        subscriptionPicker.setPlaceholder("Select a streaming subscription...");
        subscriptionPicker.setItemLabelGenerator(RegistryClient.StreamTarget::displayLabel);
        subscriptionPicker.addValueChangeListener(e -> {
            var target = e.getValue();
            if (target != null) {
                tokenField.setValue(target.streamToken() != null ? target.streamToken() : "");
            }
        });

        tokenField.setWidth("280px");
        tokenField.setPlaceholder("stk_...");

        resumeField.setWidth("130px");
        resumeField.setValue(0.0);
        resumeField.setMin(0);
        resumeField.setStepButtonsVisible(true);

        connectBtn.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
        connectBtn.addClickListener(e -> onConnect());

        disconnectBtn.addThemeVariants(ButtonVariant.LUMO_ERROR);
        disconnectBtn.addClickListener(e -> onDisconnect());

        clearBtn.addThemeVariants(ButtonVariant.LUMO_TERTIARY);
        clearBtn.addClickListener(e -> onClear());

        refreshBtn.addThemeVariants(ButtonVariant.LUMO_TERTIARY);
        refreshBtn.addClickListener(e -> loadSubscriptions());

        var row1 = new HorizontalLayout(gatewayField, subscriptionPicker, refreshBtn);
        row1.setSpacing(true);
        row1.setAlignItems(Alignment.END);

        var row2 = new HorizontalLayout(tokenField, resumeField, connectBtn, disconnectBtn, clearBtn);
        row2.setSpacing(true);
        row2.setAlignItems(Alignment.END);

        var panel = new VerticalLayout(row1, row2);
        panel.setSpacing(false);
        panel.setPadding(true);
        panel.getStyle()
            .set("background", "#1a2736")
            .set("border-radius", "8px")
            .set("border", "1px solid #2a3f55");
        return panel;
    }

    private Component buildStatusBar() {
        statusBadge.getStyle()
            .set("padding", "2px 10px")
            .set("border-radius", "4px")
            .set("font-size", "12px")
            .set("font-weight", "600");

        statsLabel.getStyle().set("color", "#8899aa").set("font-size", "13px");

        var bar = new HorizontalLayout(statusBadge, statsLabel);
        bar.setSpacing(true);
        bar.setAlignItems(Alignment.CENTER);
        return bar;
    }

    private Component buildEventGrid() {
        grid.addColumn(ReceivedEvent::position).setHeader("Pos").setWidth("70px").setFlexGrow(0);
        grid.addColumn(ReceivedEvent::eventType).setHeader("Event Type").setWidth("200px");
        grid.addColumn(ReceivedEvent::source).setHeader("Source").setWidth("150px");
        grid.addColumn(ReceivedEvent::subject).setHeader("Subject").setWidth("150px");
        grid.addColumn(e -> e.eventId() != null && e.eventId().length() > 12
                ? e.eventId().substring(0, 12) + "..." : e.eventId())
            .setHeader("Event ID").setWidth("130px");
        grid.addColumn(e -> TIME_FMT.format(e.receivedAt())).setHeader("Received").setWidth("120px");

        grid.setHeight("400px");
        grid.setWidthFull();
        grid.addThemeNames("no-border", "row-stripes");
        grid.getStyle()
            .set("background", "#1a2736")
            .set("border-radius", "8px")
            .set("border", "1px solid #2a3f55");

        grid.addItemClickListener(e -> showDetail(e.getItem()));
        return grid;
    }

    private Component buildDetailPanel() {
        detailPre.getStyle()
            .set("background", "#0f1923")
            .set("color", "#86efac")
            .set("padding", "12px")
            .set("border-radius", "4px")
            .set("font-size", "12px")
            .set("max-height", "250px")
            .set("overflow", "auto")
            .set("white-space", "pre-wrap")
            .set("word-break", "break-all")
            .set("margin", "0");
        detailPre.setText("Select a subscription above and click Connect.\nClick an event row to see the full CloudEvent JSON.");

        var details = new Details("Event Detail", detailPre);
        details.setOpened(true);
        details.setWidthFull();
        details.getStyle()
            .set("background", "#1a2736")
            .set("border-radius", "8px")
            .set("border", "1px solid #2a3f55");
        return details;
    }

    // --- Actions ---

    private void loadSubscriptions() {
        List<RegistryClient.StreamTarget> targets = registry.getStreamTargets();
        subscriptionPicker.setItems(targets);
        if (targets.isEmpty()) {
            Notification.show("No streaming subscriptions found. Create one in the admin UI.", 5000, Notification.Position.MIDDLE);
        } else {
            Notification.show("Found " + targets.size() + " streaming subscription(s)", 2000, Notification.Position.BOTTOM_END);
        }
    }

    private void onConnect() {
        String gateway = gatewayField.getValue();
        var target = subscriptionPicker.getValue();
        String token = tokenField.getValue();
        long resume = resumeField.getValue() != null ? resumeField.getValue().longValue() : 0;

        if (target == null) {
            Notification.show("Select a subscription from the dropdown", 3000, Notification.Position.MIDDLE);
            return;
        }
        if (token == null || token.isBlank()) {
            Notification.show("Auth token is required (auto-filled when you select a subscription)", 3000, Notification.Position.MIDDLE);
            return;
        }

        streamService.connect(gateway, target.subscriptionId(), token, resume);
    }

    private void onDisconnect() {
        streamService.disconnect();
    }

    private void onClear() {
        streamService.clearEvents();
        refreshGrid();
        detailPre.setText("Cleared");
    }

    private void showDetail(ReceivedEvent event) {
        detailPre.setText(
            "Position:  " + event.position() + "\n" +
            "Event ID:  " + event.eventId() + "\n" +
            "Type:      " + event.eventType() + "\n" +
            "Source:    " + event.source() + "\n" +
            "Subject:   " + event.subject() + "\n" +
            "Received:  " + event.receivedAt() + "\n" +
            "\n--- CloudEvent JSON ---\n\n" +
            event.rawJson()
        );
    }

    // --- Live updates ---

    private void registerListeners() {
        eventListener = event -> {
            if (ui != null) ui.access(() -> { refreshGrid(); updateStatus(); });
        };
        stateListener = state -> {
            if (ui != null) ui.access(this::updateStatus);
        };
        streamService.addEventListener(eventListener);
        streamService.addStateListener(stateListener);
    }

    private void refreshGrid() {
        grid.setItems(streamService.getEvents());
    }

    private void updateStatus() {
        var state = streamService.getState();
        statusBadge.setText(state.name());
        statusBadge.getStyle().remove("background").remove("color");
        switch (state) {
            case CONNECTED -> statusBadge.getStyle().set("background", "#22c55e").set("color", "#000");
            case CONNECTING -> statusBadge.getStyle().set("background", "#f59e0b").set("color", "#000");
            case ERROR -> statusBadge.getStyle().set("background", "#ef4444").set("color", "#fff");
            default -> statusBadge.getStyle().set("background", "#2a3f55").set("color", "#8899aa");
        }

        statsLabel.setText(String.format("Total: %d events | Last position: %d | %s",
            streamService.getTotalReceived(), streamService.getLastPosition(), streamService.getStatusMessage()));

        connectBtn.setEnabled(state == ConnectionState.DISCONNECTED || state == ConnectionState.ERROR);
        disconnectBtn.setEnabled(state == ConnectionState.CONNECTED || state == ConnectionState.CONNECTING);
    }

    @Override
    protected void onDetach(DetachEvent detachEvent) {
        super.onDetach(detachEvent);
        if (eventListener != null) streamService.removeEventListener(eventListener);
        if (stateListener != null) streamService.removeStateListener(stateListener);
    }
}
