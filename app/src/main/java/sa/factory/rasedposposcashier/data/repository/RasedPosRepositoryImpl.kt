package sa.factory.rasedposposcashier.data.repository

import sa.factory.rasedposposcashier.domain.repository.RasedPosRepository
import sa.factory.rasedposposcashier.hardware.HardwareSensorDriver
import sa.factory.rasedposposcashier.model.RasedPosItem
import sa.factory.rasedposposcashier.model.RasedPosOrder
import sa.factory.rasedposposcashier.model.RasedPosTelemetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch

class RasedPosRepositoryImpl(
    private val sensorDriver: HardwareSensorDriver
) : RasedPosRepository {

    private val _catalog = MutableStateFlow<List<RasedPosItem>>(listOf(
        RasedPosItem("1", "تخصيص ثيم احترافي", "تعديل عميق للثيمات المدفوعة باستخدام CSS و JavaScript مع تحسين سرعة التحميل", 1200.0, "متميز", "تخصيص الثيمات"),
        RasedPosItem("2", "رفع 200 منتج", "إدخال المنتجات مع تحسين SEO وتنظيم التصنيفات الفرعية", 850.0, "شامل", "رفع المنتجات"),
        RasedPosItem("3", "ربط بكسلات التواصل", "تكامل بكسلات Meta و Snapchat و TikTok مع تتبع الأحداث", 450.0, "ذكي", "ربط البكسلات")
    ))
    override val catalogFlow: Flow<List<RasedPosItem>> = _catalog.asStateFlow()

    private val _cart = MutableStateFlow<List<RasedPosItem>>(emptyList())
    override val activeCart: StateFlow<List<RasedPosItem>> = _cart.asStateFlow()

    override val telemetryFlow: Flow<RasedPosTelemetry> = sensorDriver.observePeripheralStream()
        .map { frame ->
            RasedPosTelemetry(
                peripheralName = frame.peripheralName,
                status = frame.connectionState,
                liveReading = frame.livePayload,
                batteryPct = 96,
                signalDbm = -54
            )
        }

    override fun addToCart(item: RasedPosItem) {
        val current = _cart.value.toMutableList()
        current.add(item)
        _cart.value = current
    }

    override fun removeFromCart(item: RasedPosItem) {
        val current = _cart.value.toMutableList()
        current.remove(item)
        _cart.value = current
    }

    override fun clearCart() {
        _cart.value = emptyList()
    }

    override suspend fun checkout(paymentMethod: String): RasedPosOrder {
        val currentItems = _cart.value.toList()
        val subtotal = currentItems.sumOf { it.priceSar }
        val vat = subtotal * 0.15
        val total = subtotal + vat
        val order = RasedPosOrder(
            orderId = "ORD-${System.currentTimeMillis() % 100000}",
            items = currentItems,
            subtotalSar = subtotal,
            vatSar = vat,
            totalSar = total,
            paymentMethod = paymentMethod
        )
        clearCart()
        return order
    }

    override suspend fun refreshCatalog() {
        // Offline-first sync logic
    }
}
