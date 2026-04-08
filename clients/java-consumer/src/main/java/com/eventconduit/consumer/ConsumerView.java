package com.eventconduit.consumer;

import com.eventconduit.consumer.StreamConsumerService.ConnectionState;
import com.vaadin.flow.component.*;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.details.Details;
import com.vaadin.flow.component.grid.Grid;
import com.vaadin.flow.component.html.*;
import com.vaadin.flow.component.notification.Notification;
import com.vaadin.flow.component.orderedlayout.FlexComponent;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
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
    private final TextField subscriptionField = new TextField("Subscription ID");
    private final TextField tokenField = new TextField("Auth Token");
    private final TextField resumeField = new TextField("Resume After (position)");
    private final Button connectBtn = new Button("Connect");
    private final Button disconnectBtn = new Button("Disconnect");
    private final Button clearBtn = new Button("Clear Events");

    private final Span statusBadge = new Span();
    private final Span statsLabel = new Span();
    private final VerticalLayout subscriptionList = new VerticalLayout();
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

        add(buildHeader());
        add(buildSubscriptionPicker());
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
        H2 title = new H2("Event Conduit — Java Stream Consumer");
        title.getStyle().set("margin", "0 0 8px 0");
        return title;
    }

    private Component buildSubscriptionPicker() {
        H4 heading = new H4("Available Streaming Subscriptions");
        heading.getStyle().set("margin", "0");

        Button refreshBtn = new Button("Reload", e -> loadSubscriptions());
        refreshBtn.addThemeVariants(ButtonVariant.LUMO_SMALL, ButtonVariant.LUMO_TERTIARY);

        HorizontalLayout header = new HorizontalLayout(heading, refreshBtn);
        header.setAlignItems(FlexComponent.Alignment.CENTER);

        subscriptionList.setPadding(false);
        subscriptionList.setSpacing(false);
        subscriptionList.getStyle().set("gap", "4px");

        VerticalLayout panel = new VerticalLayout(header, subscriptionList);
        panel.setPadding(true);
        panel.setSpacing(true);
        panel.getStyle()
            .set("background", "var(--lumo-contrast-5pct)")
            .set("border-radius", "8px");
        return panel;
    }

    private Component buildConnectionPanel() {
        gatewayField.setValue("ws://localhost:18090/v1/stream");
        gatewayField.setWidth("350px");

        subscriptionField.setWidth("320px");
        subscriptionField.setPlaceholder("Click a subscription above or paste ID");

        tokenField.setWidth("320px");
        tokenField.setPlaceholder("stk_...");

        resumeField.setWidth("120px");
        resumeField.setValue("0");

        connectBtn.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
        connectBtn.addClickListener(e -> onConnect());

        disconnectBtn.addThemeVariants(ButtonVariant.LUMO_ERROR);
        disconnectBtn.addClickListener(e -> onDisconnect());

        clearBtn.addThemeVariants(ButtonVariant.LUMO_TERTIARY);
        clearBtn.addClickListener(e -> onClear());

        HorizontalLayout row1 = new HorizontalLayout(gatewayField, subscriptionField, tokenField);
        row1.setSpacing(true);

        HorizontalLayout row2 = new HorizontalLayout(resumeField, connectBtn, disconnectBtn, clearBtn);
        row2.setSpacing(true);
        row2.setAlignItems(FlexComponent.Alignment.END);

        VerticalLayout panel = new VerticalLayout(row1, row2);
        panel.setSpacing(false);
        panel.setPadding(true);
        panel.getStyle()
            .set("background", "var(--lumo-contrast-5pct)")
            .set("border-radius", "8px");
        return panel;
    }

    private Component buildStatusBar() {
        statusBadge.getStyle()
            .set("padding", "3px 12px")
            .set("border-radius", "4px")
            .set("font-size", "13px")
            .set("font-weight", "600");

        statsLabel.getStyle().set("font-size", "13px");

        HorizontalLayout bar = new HorizontalLayout(statusBadge, statsLabel);
        bar.setSpacing(true);
        bar.setAlignItems(FlexComponent.Alignment.CENTER);
        return bar;
    }

    private Component buildEventGrid() {
        grid.addColumn(ReceivedEvent::position).setHeader("Pos").setWidth("80px").setFlexGrow(0);
        grid.addColumn(ReceivedEvent::eventType).setHeader("Event Type").setFlexGrow(2);
        grid.addColumn(ReceivedEvent::source).setHeader("Source").setFlexGrow(1);
        grid.addColumn(ReceivedEvent::subject).setHeader("Subject").setFlexGrow(1);
        grid.addColumn(e -> e.eventId() != null && e.eventId().length() > 12
                ? e.eventId().substring(0, 12) + "..." : e.eventId())
            .setHeader("Event ID").setFlexGrow(1);
        grid.addColumn(e -> TIME_FMT.format(e.receivedAt())).setHeader("Received").setWidth("120px");

        grid.setHeight("350px");
        grid.setWidthFull();
        grid.addItemClickListener(e -> showDetail(e.getItem()));
        return grid;
    }

    private Component buildDetailPanel() {
        detailPre.getStyle()
            .set("padding", "12px")
            .set("font-size", "12px")
            .set("max-height", "250px")
            .set("overflow", "auto")
            .set("white-space", "pre-wrap")
            .set("word-break", "break-all")
            .set("margin", "0");
        detailPre.setText("Click a subscription above, then Connect. Click a row to see the full JSON.");

        Details details = new Details("Event Detail (click a row to inspect)", detailPre);
        details.setOpened(true);
        details.setWidthFull();
        return details;
    }

    // --- Load subscriptions as clickable cards ---

    private void loadSubscriptions() {
        subscriptionList.removeAll();
        List<RegistryClient.StreamTarget> targets = registry.getStreamTargets();

        if (targets.isEmpty()) {
            subscriptionList.add(new Span("No streaming subscriptions found. Create one in the Event Conduit Admin UI (http://localhost:18085)."));
            return;
        }

        for (var target : targets) {
            String types = String.join(", ", target.eventTypes());
            String label = target.subscriberName() + " — [" + types + "]";
            String subLabel = "ID: " + target.subscriptionId();

            Button selectBtn = new Button("Use this subscription", click -> {
                subscriptionField.setValue(target.subscriptionId());
                tokenField.setValue(target.streamToken() != null ? target.streamToken() : "");
                Notification.show("Selected: " + target.subscriberName(), 2000, Notification.Position.BOTTOM_END);
            });
            selectBtn.addThemeVariants(ButtonVariant.LUMO_SMALL, ButtonVariant.LUMO_PRIMARY);

            Span nameSpan = new Span(label);
            nameSpan.getStyle().set("font-weight", "600");
            Span idSpan = new Span(subLabel);
            idSpan.getStyle().set("font-size", "12px").set("color", "var(--lumo-secondary-text-color)");

            HorizontalLayout row = new HorizontalLayout();
            row.setWidthFull();
            row.setAlignItems(FlexComponent.Alignment.CENTER);

            VerticalLayout info = new VerticalLayout(nameSpan, idSpan);
            info.setPadding(false);
            info.setSpacing(false);

            row.add(info, selectBtn);
            row.expand(info);
            row.getStyle()
                .set("padding", "8px 12px")
                .set("background", "var(--lumo-contrast-5pct)")
                .set("border-radius", "6px");

            subscriptionList.add(row);
        }
    }

    // --- Actions ---

    private void onConnect() {
        String gateway = gatewayField.getValue();
        String subId = subscriptionField.getValue();
        String token = tokenField.getValue();
        long resume = 0;
        try { resume = Long.parseLong(resumeField.getValue()); } catch (Exception ignored) {}

        if (subId == null || subId.isBlank()) {
            Notification.show("Click 'Use this subscription' above to select one, or paste a subscription ID", 4000, Notification.Position.MIDDLE);
            return;
        }
        if (token == null || token.isBlank()) {
            Notification.show("Auth token is required", 3000, Notification.Position.MIDDLE);
            return;
        }

        streamService.connect(gateway, subId, token, resume);
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
            "Received:  " + event.receivedAt() + "\n\n" +
            "--- CloudEvent JSON ---\n\n" +
            event.rawJson()
        );
    }

    // --- Live updates via @Push ---

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
        ConnectionState state = streamService.getState();
        statusBadge.setText(state.name());

        switch (state) {
            case CONNECTED -> {
                statusBadge.getStyle().set("background", "var(--lumo-success-color)").set("color", "white");
            }
            case CONNECTING -> {
                statusBadge.getStyle().set("background", "#f59e0b").set("color", "black");
            }
            case ERROR -> {
                statusBadge.getStyle().set("background", "var(--lumo-error-color)").set("color", "white");
            }
            default -> {
                statusBadge.getStyle().set("background", "var(--lumo-contrast-20pct)").set("color", "var(--lumo-body-text-color)");
            }
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
