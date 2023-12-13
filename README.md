@Component
public class TimeRestrictionFilter implements Filter {

    private static final LocalTime START_TIME = LocalTime.of(9, 0);
    private static final LocalTime END_TIME = LocalTime.of(13, 0);

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        LocalTime now = LocalTime.now();
        if (now.isAfter(START_TIME) && now.isBefore(END_TIME)) {
            HttpServletResponse httpResponse = (HttpServletResponse) response;
            httpResponse.setStatus(HttpStatus.FORBIDDEN.value());
            return;
        }
        chain.doFilter(request, response);
    }
}

@Configuration
public class AppConfig {

    @Bean
    public FilterRegistrationBean<TimeRestrictionFilter> timeRestrictionFilterRegistration() {
        FilterRegistrationBean<TimeRestrictionFilter> registration = new FilterRegistrationBean<>();
        registration.setFilter(new TimeRestrictionFilter());
        registration.addUrlPatterns("/my-endpoint/*"); // Replace with your endpoint URL pattern
        return registration;
    }
}# spring-batch2-demo

